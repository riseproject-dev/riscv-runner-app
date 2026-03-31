import os
import re
import sys
import time

import logging
# logging.basicConfig(level=logging.INFO)

from enum import StrEnum

from fabric import Connection
from paramiko.ssh_exception import NoValidConnectionsError, SSHException

from scaleway import Client
from scaleway.instance.v1.custom_api import InstanceUtilsV1API
from scaleway.instance.v1.types import VolumeServerTemplate, VolumeVolumeType, ServerAction
from scaleway.baremetal.v1 import BaremetalV1API
from scaleway.baremetal.v1.content import SERVER_TRANSIENT_STATUSES, SERVER_INSTALL_TRANSIENT_STATUSES
from scaleway.baremetal.v1.types import CreateServerRequestInstall
from scaleway.baremetal.v3 import BaremetalV3PrivateNetworkAPI
from scaleway.ipam.v1 import IpamV1API
from scaleway.ipam.v1.types import ResourceType
from scaleway_core.utils import WaitForOptions
from scaleway_core.api import ScalewayException

assert os.path.dirname(os.path.abspath(__file__)) in sys.path
from constants import *

class ProvisioningException(Exception):
    pass

# --- Scaleway SDK clients ---

scw_client = Client.from_config_file_and_env()
scw_client.default_zone = ZONE
scw_client.default_project_id = PROJECT_ID
instance_api = InstanceUtilsV1API(scw_client)
baremetal_api = BaremetalV1API(scw_client)
baremetal_pn_api = BaremetalV3PrivateNetworkAPI(scw_client)
ipam_api = IpamV1API(scw_client)


# --- SSH helpers via fabric ---

def ssh_connect(host, user, retries=30, delay=30):
    """Wait for SSH to be available and return a fabric Connection."""
    assert host, "host must be defined"
    assert user, "user must be defined"
    for attempt in range(retries):
        try:
            conn = Connection(
                host,
                user=user,
                connect_kwargs={
                    "key_filename": "/Users/luhenry/.ssh/id_rivos",
                },
            )
            conn.run("true")
            return conn
        except (NoValidConnectionsError, SSHException, OSError, TimeoutError) as e:
            print(f"SSH not ready (attempt {attempt + 1}/{retries}), error: \"{e}\". Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"SSH to {user}@{host} not available after {retries} attempts")


# --- IPAM helpers ---

def get_private_ip_for_nic(nic_id):
    """Get the IPv4 address assigned to an instance private NIC via IPAM."""
    resp = ipam_api.list_i_ps(
        resource_id=nic_id,
        resource_type=ResourceType.INSTANCE_PRIVATE_NIC,
        is_ipv6=False,
    )
    for ip in resp.ips:
        if not ip.is_ipv6:
            return ip.address.split("/")[0]

    raise ProvisioningException(f"No IPv4 address assigned via IPAM for NIC {nic_id}")


# --- Private network result types ---

class PrivateNetwork:
    def __init__(self, ip):
        self.ip = ip

class InstancePrivateNetwork(PrivateNetwork):
    def __init__(self, ip):
        super().__init__(ip)

class BareMetalPrivateNetwork(PrivateNetwork):
    def __init__(self, ip, vlan_id):
        super().__init__(ip)
        self.vlan_id = vlan_id


# --- Server wrappers ---

class Instance:
    def __init__(self, id):
        self.id = id

    @staticmethod
    def create(hostname, server_type: str, storage_size: int, cloud_init_script: str):
        resp = instance_api.create_server(
            commercial_type=server_type,
            name=hostname,
            image="ubuntu_noble",
            volumes={"0": VolumeServerTemplate(
                volume_type=VolumeVolumeType.SBS_VOLUME,
                size=storage_size,
            )},
        )
        server_id = resp.server.id

        # Set cloud-init user data
        instance_api.set_server_user_data(
            server_id=server_id,
            key="cloud-init",
            content=cloud_init_script.encode(),
        )

        # Power on the server and wait for it to be running
        instance_api.server_action(server_id=server_id, action=ServerAction.POWERON)
        instance_api.wait_instance_server(server_id=server_id, zone=ZONE) # it doesn't take zone from default

        return Instance(server_id)

    def get_public_ip(self):
        resp = instance_api.get_server(server_id=self.id)
        server = resp.server
        if server.public_ip and server.public_ip.address:
            return server.public_ip.address
        for ip in (server.public_ips or []):
            if ip.address:
                return ip.address
        raise RuntimeError(f"No public IP found for instance {self.id}")

    def attach_private_network(self):
        resp = instance_api.create_private_nic(
            server_id=self.id,
            private_network_id=PRIVATE_NETWORK_ID,
        )
        nic_id = resp.private_nic.id
        ip = get_private_ip_for_nic(nic_id)
        return InstancePrivateNetwork(ip)

    def delete(self):
        instance_api.server_action(server_id=self.id, action=ServerAction.TERMINATE)


class BareMetal:
    def __init__(self, id):
        self.id = id

    @staticmethod
    def create(hostname, server_type, os_id, tags=None):
        # Look up the offer ID by name
        offers_resp = baremetal_api.list_offers(zone=ZONE)
        offer_id = None
        for offer in offers_resp.offers:
            if offer.name == server_type:
                offer_id = offer.id
                break
        if not offer_id:
            raise RuntimeError(f"Offer '{server_type}' not found")

        server = baremetal_api.create_server(
            name=hostname,
            description="",
            protected=False,
            offer_id=offer_id,
            tags=tags or [],
            install=CreateServerRequestInstall(
                os_id=os_id,
                hostname=hostname,
                ssh_key_ids=SSH_KEY_IDS,
            ),
        )

        return BareMetal(server.id)

    def start(self):
        baremetal_api.start_server(server_id=self.id)

    def get_public_ip(self):
        server = baremetal_api.get_server(server_id=self.id)
        for ip in (server.ips or []):
            if ip.version == "IPv4":
                return ip.address
        raise ProvisioningException(f"No IPv4 address found for baremetal server {self.id}")

    def attach_private_network(self):
        # Enable private network option
        options_resp = baremetal_api.list_options(zone=ZONE)
        option_id = None
        for option in options_resp.options:
            if option.name == "Private Network":
                option_id = option.id
                break
        if not option_id:
            raise ProvisioningException("Private Network option not found")

        try:
            baremetal_api.add_option_server(server_id=self.id, option_id=option_id)
        except ScalewayException:
            # Ignore if the option is already on the server
            pass

        time.sleep(1) # there are timing issues sometimes leading to 500

        # Attach to the private network
        pn = baremetal_pn_api.add_server_private_network(
            server_id=self.id,
            private_network_id=PRIVATE_NETWORK_ID,
        )

        for ipam_ip_id in (pn.ipam_ip_ids or []):
            ip_info = ipam_api.get_ip(ip_id=ipam_ip_id)
            if not ip_info.is_ipv6:
                return BareMetalPrivateNetwork(ip_info.address, pn.vlan)

        raise ProvisioningException(f"No private IPv4 address assigned for baremetal server {self.id}")

    def get_private_network(self):
        pn_resp = baremetal_pn_api.list_server_private_networks(
            server_id=self.id,
        )
        for pn in pn_resp.server_private_networks:
            for ipam_ip_id in (pn.ipam_ip_ids or []):
                ip_info = ipam_api.get_ip(ip_id=ipam_ip_id)
                if not ip_info.is_ipv6:
                    return BareMetalPrivateNetwork(ip_info.address, pn.vlan)
        raise ProvisioningException(f"No private IPv4 address found for baremetal server {self.id}")

    def update_tags(self, tags):
        baremetal_api.update_server(server_id=self.id, tags=tags)

    def reinstall(self, os_id, hostname):
        baremetal_api.install_server(
            server_id=self.id,
            os_id=os_id,
            hostname=hostname,
            ssh_key_ids=SSH_KEY_IDS,
        )

    def delete(self):
        baremetal_api.delete_server(server_id=self.id)

    def wait_for_server(self):
        def is_ready(res):
            ready = res.status not in SERVER_TRANSIENT_STATUSES and res.install.status not in SERVER_INSTALL_TRANSIENT_STATUSES
            print(f"  server status = {res.status}, server install status = {res.install.status}, {"ready!" if ready else "not ready yet!"}")
            return ready

        time.sleep(5) # there can be a race condition between the previous operation
                      # and waiting for the server, add an artificial sleep to allow
                      # scaleway's backend to sync up
        baremetal_api.wait_for_server(
            server_id=self.id,  
            options=WaitForOptions(
                timeout=15*60, # 15 minutes
                stop=is_ready,
            ),
        )
