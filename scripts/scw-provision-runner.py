#!/usr/bin/env python3
"""Provision RISE RISC-V runner bare metal servers on Scaleway."""

import argparse
import itertools
import os
import re
import sys
import time

if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import *
from constants import *

from invoke.exceptions import UnexpectedExit

SERVER_TYPE = "EM-RV1-C4M16S128-A"

RETRY_DELAY = 60

SETUP_SCRIPT = r"""
# Redirect stdout and stderr to /var/log/riscv-runner-setup.log
exec > >(sudo tee /var/log/riscv-runner-setup.log) 2>&1

set -euxo pipefail

# Fresh packages
sudo apt update -qq
sudo apt upgrade -qq -y

# Load required kernel modules
cat <<EOF | sudo tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

sudo modprobe overlay
sudo modprobe br_netfilter

# Configure sysctl params for Kubernetes networking
cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

# Apply the changes
sudo sysctl --system

# # Configure private network VLAN interface
# cat <<'EOF' | sudo tee -a /etc/systemd/network/05-end0.network
# [Match]
# Name=end0
# [Network]
# DHCP=yes
# VLAN=end0.@@PN_VLAN_ID@@
# EOF

# cat <<'EOF' | sudo tee /etc/systemd/network/10-end0.@@PN_VLAN_ID@@.netdev
# [NetDev]
# Name=end0.@@PN_VLAN_ID@@
# Kind=vlan
# [VLAN]
# Id=@@PN_VLAN_ID@@
# EOF

# cat <<'EOF' | sudo tee /etc/systemd/network/11-end0.@@PN_VLAN_ID@@.network
# [Match]
# Name=end0.@@PN_VLAN_ID@@
# [Network]
# Address=@@PN_IP@@
# EOF

# sudo networkctl reload

# Check that it succeeded
# sudo apt install -qq -y --no-install-recommends retry
# retry --delay=2 --times=5 -- ip addr show end0.@@PN_VLAN_ID@@

# # Configure private network VLAN interface
# sudo ip link add link end0 name end0.@@PN_VLAN_ID@@ type vlan id @@PN_VLAN_ID@@
# sudo ip link set end0.@@PN_VLAN_ID@@ up
# sudo ip addr add @@PN_IP@@ dev end0.@@PN_VLAN_ID@@

# Install containerd
sudo apt install -qq -y --no-install-recommends containerd
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null

# 1. Enable SystemdCgroup driver
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/g' /etc/containerd/config.toml

# 2. Set the multi-arch (amd64/riscv64) compatible pause image
# This ensures that both architectures can pull a valid sandbox image
sudo sed -i 's|sandbox_image = ".*"|sandbox_image = "cloudv10x/pause:3.10"|' /etc/containerd/config.toml

# 3. Restart the service
sudo systemctl restart containerd

sudo apt install -qq -y --no-install-recommends curl unzip
curl -fsSL \
  --retry 5 \
  --retry-delay 5 \
  --retry-all-errors \
  -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0" \
  -H "Accept: */*" \
  -H "Referer: https://gitlab.com/" \
  -o artifacts.zip \
  https://gitlab.com/riseproject/risc-v-runner/kubernetes/-/jobs/13257210986/artifacts/download
unzip artifacts.zip '_output/*' -d artifacts
sudo mv artifacts/_output/local/go/bin/kube* /usr/local/bin/
rm -rf artifacts artifacts.zip
sudo chown root:root /usr/local/bin/kube*
sudo chmod +x /usr/local/bin/kube*

sudo mkdir -p /opt/cni/bin
curl -fsSL \
  --retry 5 \
  --retry-delay 5 \
  --retry-all-errors \
  https://github.com/containernetworking/plugins/releases/download/v1.4.0/cni-plugins-linux-riscv64-v1.4.0.tgz | \
    sudo tar -C /opt/cni/bin -xvzf -

cat <<'EOF' | sudo tee /etc/systemd/system/kubelet.service
[Unit]
Description=kubelet: The Kubernetes Node Agent
Documentation=https://kubernetes.io/docs/
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/kubelet
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo mkdir -p /etc/systemd/system/kubelet.service.d

cat <<'EOF' | sudo tee /etc/systemd/system/kubelet.service.d/10-kubeadm.conf
[Service]
Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
EnvironmentFile=-/etc/default/kubelet
ExecStart=
ExecStart=/usr/local/bin/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS $KUBELET_EXTRA_ARGS
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now kubelet


# Join the cluster (uses the control plane's private network IP)
sudo kubeadm reset -f || true
sudo @@KUBEADM_JOIN_CMD@@

# Mandatory reboot for fresh nodes to finalize networking and cgroups
sudo reboot
"""


def get_control_plane_host(control_plane_name):
    """Returns (public_ip, private_ip)."""
    resp = instance_api.list_servers(name=control_plane_name)
    for server in resp.servers:
        if server.name == control_plane_name:
            # Get public IP
            public_ip = None
            if server.public_ip and server.public_ip.address:
                public_ip = server.public_ip.address
            if not public_ip:
                for ip in (server.public_ips or []):
                    if ip.address:
                        public_ip = ip.address
                        break
            if not public_ip:
                raise RuntimeError(f"Control plane '{control_plane_name}' has no public IP")

            # Get private IP from the private NIC via IPAM
            private_ip = None
            for nic in (server.private_nics or []):
                ip_resp = ipam_api.list_i_ps(
                    resource_id=nic.id,
                    resource_type=ResourceType.INSTANCE_PRIVATE_NIC,
                    project_id=PROJECT_ID,
                    is_ipv6=False,
                )
                for ip_info in ip_resp.ips:
                    if not ip_info.is_ipv6:
                        private_ip = ip_info.address.split("/")[0]
                        break
                if private_ip:
                    break
            if not private_ip:
                raise RuntimeError(f"Control plane '{control_plane_name}' has no private IP")

            return public_ip, private_ip
    raise RuntimeError(f"Control plane '{control_plane_name}' not found in project {PROJECT_ID}")


def get_os_id():
    resp = baremetal_api.list_os()
    for os_entry in resp.os:
        if os_entry.name == "Ubuntu" and os_entry.version == "24.04 LTS (Noble Numbat)":
            return os_entry.id
    raise RuntimeError("Ubuntu 24.04 LTS OS not found")


def get_kubeadm_join_cmd(ssh_cp, cp_ip):
    # Create a short-lived token
    result = ssh_cp.run("kubeadm token create --ttl 15m", hide=True)
    token = result.stdout.strip()

    # Get the CA cert hash
    result = ssh_cp.run(
        "openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt"
        " | openssl rsa -pubin -outform der 2>/dev/null"
        " | openssl dgst -sha256 -hex"
        " | sed 's/^.* //'",
        hide=True,
    )
    ca_cert_hash = result.stdout.strip()

    return f"kubeadm join {cp_ip}:6443 --token {token} --discovery-token-ca-cert-hash sha256:{ca_cert_hash}"


def run_setup(ssh, pn, ssh_cp, cp_public_ip):
    join_cmd = get_kubeadm_join_cmd(ssh_cp, cp_public_ip)
    script = SETUP_SCRIPT.replace("@@KUBEADM_JOIN_CMD@@", join_cmd) \
                         #FIXME(pn): enable private address again
                         # .replace("@@PN_IP@@", pn.ip)
                         # .replace("@@PN_VLAN_ID@@", pn.vlan_id)
    ssh.run(script)


def find_server_by_name(hostname):
    resp = baremetal_api.list_servers(name=hostname)
    for server in resp.servers:
        if server.name == hostname:
            return server.id
    raise RuntimeError(f"Server '{hostname}' not found in project {PROJECT_ID}")


def drain_and_delete_k8s_node(hostname, ssh_cp):
    ssh_cp.run(
        f"kubectl --kubeconfig=/etc/kubernetes/admin.conf drain {hostname} --ignore-daemonsets --delete-emptydir-data --force --timeout=0",
        warn=True,
    )
    ssh_cp.run(
        f"kubectl --kubeconfig=/etc/kubernetes/admin.conf delete node {hostname} --ignore-not-found",
    )


def wait_for_k8s_node(hostname, ssh_cp):
    while True:
        try:
            result = ssh_cp.run(f"kubectl --kubeconfig=/etc/kubernetes/admin.conf get node {hostname} --no-headers -o name", hide='both')
            assert result.exited == 0
            print(f"  node {hostname} available but not ready yet!")
            break
        except UnexpectedExit:
            print(f"  node {hostname} not available yet!")
            time.sleep(15)

    ssh_cp.run(
        f"kubectl --kubeconfig=/etc/kubernetes/admin.conf wait --for=condition=Ready node/{hostname} --timeout=600s", hide='out'
    )
    print(f"  node {hostname} available and ready!")


def create_server(hostname, os_id, tags=None):
    while True:
        try:
            return BareMetal.create(hostname, SERVER_TYPE, os_id, tags=tags)
        except Exception:
            print(f"Server creation failed, retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)


def get_next_runner_index():
    prefix = "riscv-runner-"
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+)$")
    used = set()
    # baremetal_api.list_servers uses pagination
    for page in itertools.count(start=0):
        resp = baremetal_api.list_servers(page=page)
        if len(resp.servers) == 0:
            break
        for server in resp.servers:
            m = pattern.match(server.name or "")
            if m:
                used.add(int(m.group(1)))
    i = 0
    while i in used:
        i += 1
    return i


def cmd_create(args):
    cp_public_ip, cp_private_ip = get_control_plane_host(args.control_plane)
    print(f"Using control plane: {cp_public_ip} (private: {cp_private_ip})")
    ssh_cp = ssh_connect(host=cp_public_ip, user="root")

    os_id = get_os_id()
    print(f"Using OS ID: {os_id}")

    for _ in range(args.count):
        index = get_next_runner_index()
        runner = f"riscv-runner-{index}"
        print(f"\n{'='*60}")
        print(f"Creating runner {runner}")
        print(f"{'='*60}")

        tags = [f"control-plane:{args.control_plane}"]
        print(f"Provisioning {runner}")
        server = create_server(runner, os_id, tags=tags)
        server.wait_for_server()
        print(f"Server created: {server.id}")

        #FIXME(pn): Disable private network for now, it doesn't work reliably enough
        # pn = server.attach_private_network()
        # print(f"Private network enabled (VLAN {pn.vlan_id}, IP {pn.ip})")
        pn = None

        print(f"Starting {runner}...")
        server.start()
        server.wait_for_server()
        ip = server.get_public_ip()
        print(f"Server IP: {ip}")

        ssh = ssh_connect(host=ip, user="ubuntu")
        run_setup(ssh, pn, ssh_cp, cp_public_ip)

        print(f"Waiting for node {runner} to be ready in k8s")
        wait_for_k8s_node(runner, ssh_cp)

        print(f"Server {runner} provisioned")


def cmd_reinstall(args):
    cp_public_ip, cp_private_ip = get_control_plane_host(args.control_plane)
    print(f"Using control plane: {cp_public_ip} (private: {cp_private_ip})")
    ssh_cp = ssh_connect(host=cp_public_ip, user="root")

    os_id = get_os_id()
    print(f"Using OS ID: {os_id}")

    for runner in args.runners:
        if args.rename:
            index = get_next_runner_index()
            new_name = f"riscv-runner-{index}"
        else:
            new_name = runner

        print(f"\n{'='*60}")
        print(f"Reinstalling runner {runner}" + (f" (renaming to {new_name})" if new_name != runner else ""))
        print(f"{'='*60}")

        server_id = find_server_by_name(runner)
        print(f"Found existing server: {server_id}")

        print(f"Draining and removing {runner} from k8s")
        drain_and_delete_k8s_node(runner, ssh_cp)
        print(f"Drained and removed {runner} from k8s")

        server = BareMetal(server_id)

        if new_name != runner:
            server.rename(new_name)
            print(f"Renamed server to {new_name}")

        tags = [f"control-plane:{args.control_plane}"]
        server.update_tags(tags)
        print(f"Tags updated: {tags}")

        print(f"Reinstalling OS on {new_name}...")
        server.reinstall(os_id, new_name)
        server.wait_for_server()
        print(f"OS reinstalled on {new_name}")

        #FIXME(pn): Disable private network for now, it doesn't work reliably enough
        # try:
        #     pn = server.get_private_network()
        # except ProvisioningException:
        #     pn = server.attach_private_network()
        # print(f"Private IP: {pn.ip}, vlan={pn.vlan_id}")
        pn = None

        ip = server.get_public_ip()
        print(f"Public IP: {ip}")

        ssh = ssh_connect(host=ip, user="ubuntu")
        run_setup(ssh, pn, ssh_cp, cp_public_ip)

        print(f"Waiting for node {new_name} to be ready on k8s")
        wait_for_k8s_node(new_name, ssh_cp)

        print(f"Server {new_name} provisioned")


def cmd_list(args):
    tag = f"control-plane:{args.control_plane}"
    servers = baremetal_api.list_servers_all(tags=[tag])

    rows = []
    for s in servers:
        install_status = s.install.status if s.install else "unknown"
        tags = ",".join(s.tags)
        rows.append((s.id, s.name, s.status, install_status, tags, s.ping_status))

    # Compute column widths
    headers = ("ID", "NAME", "STATUS", "INSTALL", "TAGS", "PING")
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))


def cmd_delete(args):
    cp_public_ip, cp_private_ip = get_control_plane_host(args.control_plane)
    print(f"Using control plane: {cp_public_ip} (private: {cp_private_ip})")
    ssh_cp = ssh_connect(host=cp_public_ip, user="root")

    for runner in args.runners:
        print(f"\n{'='*60}")
        print(f"Deleting runner {runner}")
        print(f"{'='*60}")

        server_id = find_server_by_name(runner)
        print(f"Found server: {server_id}")

        print(f"Draining and removing {runner} from k8s")
        drain_and_delete_k8s_node(runner, ssh_cp)
        print(f"Drained and removed {runner} from k8s")

        server = BareMetal(server_id)
        server.delete()
        print(f"Server {runner} deleted")


def main():
    parser = argparse.ArgumentParser(description="Provision RISE RISC-V runners on Scaleway")
    parser.add_argument("--control-plane", type=str, required=True, help="Name of the control plane instance")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create new runners")
    create_parser.add_argument("count", nargs="?", type=int, default=1, help="Number of new runners to create")

    subparsers.add_parser("list", help="List runners")

    reinstall_parser = subparsers.add_parser("reinstall", help="Reinstall OS on existing runners")
    reinstall_parser.add_argument("runners", nargs="+", type=str, help="Runner to reinstall")
    reinstall_parser.add_argument("--rename", action="store_true", help="Rename the runner")

    delete_parser = subparsers.add_parser("delete", help="Delete existing runners")
    delete_parser.add_argument("runners", nargs="+", type=str, help="Runners to delete")

    args = parser.parse_args()

    if args.command == "list":
        cmd_list(args)
    elif args.command == "create":
        cmd_create(args)
    elif args.command == "reinstall":
        cmd_reinstall(args)
    elif args.command == "delete":
        cmd_delete(args)
    else:
        print(f"unknown command {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
