#!/usr/bin/env python3
"""Provision RISE RISC-V runner bare metal servers on Scaleway."""

import argparse
import json
import re
import subprocess
import sys
import time

ZONE = "fr-par-2"
PROJECT_ID = "03a2e06e-e7c1-45a6-9f05-775d813c2e28"
SERVER_TYPE = "EM-RV1-C4M16S128-A"
CONTROL_PLANE_HOST = "root@51.159.186.52"

SSH_KEY_IDS = [
    "14243d19-acaa-4d67-976c-8cd417fb613a", # Ludovic Henry
    "56ccd923-1ea3-4398-b8ee-7c1a930cbe75", # Ludovic Henry
]
RETRY_DELAY = 60

SETUP_SCRIPT = r"""
set -euxo pipefail

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

# Configure private network VLAN interface
sudo ip link add link end0 name end0.@@VLAN_ID@@ type vlan id @@VLAN_ID@@
sudo ip link set end0.@@VLAN_ID@@ up
sudo ip addr add @@VLAN_IPV4@@ dev end0.@@VLAN_ID@@

# Install containerd
sudo apt update && sudo apt install -y containerd
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null

# 1. Enable SystemdCgroup driver
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/g' /etc/containerd/config.toml

# 2. Set the multi-arch (amd64/riscv64) compatible pause image
# This ensures that both architectures can pull a valid sandbox image
sudo sed -i 's|sandbox_image = ".*"|sandbox_image = "cloudv10x/pause:3.10"|' /etc/containerd/config.toml

# 3. Restart the service
sudo systemctl restart containerd

sudo apt install wget unzip -y
wget --progress=dot:giga https://gitlab.com/riseproject/risc-v-runner/kubernetes/-/jobs/13257210986/artifacts/download -O artifacts.zip
unzip artifacts.zip '_output/*' -d artifacts
sudo mv artifacts/_output/local/go/bin/kube* /usr/local/bin/
rm -rf artifacts artifacts.zip
sudo chown root:root /usr/local/bin/kube*
sudo chmod +x /usr/local/bin/kube*

sudo mkdir -p /opt/cni/bin
curl -L https://github.com/containernetworking/plugins/releases/download/v1.4.0/cni-plugins-linux-riscv64-v1.4.0.tgz | \
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

# Setup kubeadm
sudo kubeadm join 51.159.186.52:6443 --token @@KUBEADM_JOIN_TOKEN@@ --discovery-token-ca-cert-hash sha256:a4c46a2bb5f33cea61fe2b1e98b14dd451bdaf0429763a2685762364c4e37cfa

# Mandatory reboot for fresh nodes to finalize networking and cgroups
sudo reboot
"""


def run(cmd, *, check=True, capture=True):
    print(f"\033[32m+ {' '.join(cmd)}\033[0m")
    return subprocess.run(cmd, stdout=subprocess.PIPE if capture else None, text=True, check=check)


def get_private_network_option_id():
    result = run(["scw", "baremetal", "options", "list", f"zone={ZONE}", "-o", "json"])
    for option in json.loads(result.stdout):
        if option["name"] == "Private Network":
            return option["id"]
    raise RuntimeError("Private Network option not found")


def get_private_network_id():
    result = run(["scw", "vpc", "private-network", "list", "-o", "json"])
    for pn in json.loads(result.stdout):
        if pn["name"] == "rpvn-rise-riscv-runner-app":
            return pn["id"]
    raise RuntimeError("Private network 'rpvn-rise-riscv-runner-app' not found")


def enable_private_network(server_id, option_id, private_network_id):
    run(["scw", "baremetal", "options", "add", f"zone={ZONE}", f"server-id={server_id}", f"option-id={option_id}"])
    result = run(["scw", "baremetal", "private-network", "add", f"zone={ZONE}", f"server-id={server_id}", f"private-network-id={private_network_id}", "-o", "json"])
    pn = json.loads(result.stdout)
    pn_vlan_id = pn["vlan"]
    for ipam_ip_id in pn.get("ipam_ip_ids", []):
        ip_result = run(["scw", "ipam", "ip", "get", ipam_ip_id, "-o", "json"])
        ip_info = json.loads(ip_result.stdout)
        if not ip_info["is_ipv6"]:
            return pn_vlan_id, ip_info["address"]
    raise RuntimeError(f"No IPv4 address assigned via IPAM for server {server_id}")


def get_os_id():
    result = run(["scw", "baremetal", "os", "list", f"zone={ZONE}", "-o", "json"])
    for os_entry in json.loads(result.stdout):
        if os_entry["name"] == "Ubuntu" and os_entry["version"] == "24.04 LTS (Noble Numbat)":
            return os_entry["id"]
    raise RuntimeError("Ubuntu 24.04 LTS OS not found")


def get_kubeadm_join_token():
    result = run(["ssh", CONTROL_PLANE_HOST, "kubeadm", "token", "create", "--ttl", "5m"])
    return result.stdout.strip()


def get_server_ip(server_id):
    result = run(["scw", "baremetal", "server", "get", server_id, f"zone={ZONE}", "-o", "json"])
    server = json.loads(result.stdout)
    for ip in server.get("ips", []):
        if ip.get("version") == "IPv4":
            return ip["address"]
    raise RuntimeError(f"No IPv4 address found for server {server_id}")


def wait_for_ssh(host, retries=30, delay=30):
    for attempt in range(retries):
        result = run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10", f"ubuntu@{host}", "true"],
            check=False, capture=True,
        )
        if result.returncode == 0:
            return
        print(f"SSH not ready (attempt {attempt + 1}/{retries}), retrying in {delay}s...")
        time.sleep(delay)
    raise RuntimeError(f"SSH to {host} not available after {retries} attempts")


def run_setup(host, pn_vlan_id, pn_ipv4):
    script = SETUP_SCRIPT.replace("@@KUBEADM_JOIN_TOKEN@@", get_kubeadm_join_token()) \
                         .replace("@@VLAN_ID@@", str(pn_vlan_id)) \
                         .replace("@@VLAN_IPV4@@", pn_ipv4)
    run(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{host}", "bash", "-c", script], capture=False)


def find_server_by_name(hostname):
    result = run(["scw", "baremetal", "server", "list", f"zone={ZONE}", f"project-id={PROJECT_ID}", f"name={hostname}", "-o", "json"])
    servers = json.loads(result.stdout)
    for server in servers:
        if server["name"] == hostname:
            return server["id"]
    raise RuntimeError(f"Server '{hostname}' not found in project {PROJECT_ID}")


def drain_and_delete_k8s_node(hostname):
    run(["ssh", CONTROL_PLANE_HOST, "kubectl", "--kubeconfig=/etc/kubernetes/admin.conf", "drain", hostname,
         "--ignore-daemonsets", "--delete-emptydir-data", "--force"], check=False)
    run(["ssh", CONTROL_PLANE_HOST, "kubectl", "--kubeconfig=/etc/kubernetes/admin.conf", "delete", "node", hostname, "--ignore-not-found"])


def create_server(hostname, os_id):
    cmd = [
        "scw", "baremetal", "server", "create",
        f"zone={ZONE}",
        f"project-id={PROJECT_ID}",
        f"type={SERVER_TYPE}",
        f"name={hostname}",
        f"install.hostname={hostname}",
        f"install.os-id={os_id}",
    ]
    for i, key_id in enumerate(SSH_KEY_IDS):
        cmd.append(f"install.ssh-key-ids.{i}={key_id}")
    cmd.extend(["--wait", "-o", "json"])

    while True:
        result = run(cmd, check=False)
        if result.returncode == 0:
            return json.loads(result.stdout)["id"]
        print(f"Server creation failed, retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)


def start_server(server_id):
    run(["scw", "baremetal", "server", "start", server_id, f"zone={ZONE}", "--wait"])


def delete_server(server_id):
    run(["scw", "baremetal", "server", "delete", server_id, f"zone={ZONE}"])


def get_next_runner_index():
    result = run(["scw", "baremetal", "server", "list", f"zone={ZONE}", f"project-id={PROJECT_ID}", "-o", "json"])
    servers = json.loads(result.stdout)
    used = set()
    for server in servers:
        m = re.match(r"^rise-riscv-runner-(\d+)$", server.get("name", ""))
        if m:
            used.add(int(m.group(1)))
    i = 1
    while i in used:
        i += 1
    return i


def cmd_create(args):
    os_id = get_os_id()
    print(f"Using OS ID: {os_id}")
    pn_option_id = get_private_network_option_id()
    pn_id = get_private_network_id()

    for _ in range(args.count):
        index = get_next_runner_index()
        hostname = f"rise-riscv-runner-{index}"
        print(f"\n{'='*60}")
        print(f"Creating runner {hostname}")
        print(f"{'='*60}")

        server_id = create_server(hostname, os_id)
        print(f"Server created: {server_id}")

        pn_vlan_id, pn_ipv4 = enable_private_network(server_id, pn_option_id, pn_id)
        print(f"Private network enabled (VLAN {pn_vlan_id}, IP {pn_ipv4})")

        start_server(server_id)
        ip = get_server_ip(server_id)
        print(f"Server IP: {ip}")

        wait_for_ssh(ip)
        run_setup(ip, pn_vlan_id, pn_ipv4)
        print(f"Server {hostname} provisioned")


def cmd_reinstall(args):
    os_id = get_os_id()
    print(f"Using OS ID: {os_id}")
    pn_option_id = get_private_network_option_id()
    pn_id = get_private_network_id()

    for runner_id in args.ids:
        hostname = f"rise-riscv-runner-{runner_id}"

        print(f"\n{'='*60}")
        print(f"Reinstalling runner {hostname}")
        print(f"{'='*60}")

        old_server_id = find_server_by_name(hostname)
        print(f"Found existing server: {old_server_id}")

        drain_and_delete_k8s_node(hostname)
        print("Drained and removed from Kubernetes cluster")

        print(f"Deleting old server {hostname}...")
        delete_server(old_server_id)
        print(f"Old server {old_server_id} deleted")

        print(f"Creating new server {hostname}...")
        new_server_id = create_server(hostname, os_id)
        print(f"New server created: {new_server_id}")

        pn_vlan_id, pn_ipv4 = enable_private_network(new_server_id, pn_option_id, pn_id)
        print(f"Private network enabled (VLAN {pn_vlan_id}, IP {pn_ipv4})")

        start_server(new_server_id)
        ip = get_server_ip(new_server_id)
        print(f"Server IP: {ip}")

        wait_for_ssh(ip)
        run_setup(ip, pn_vlan_id, pn_ipv4)
        print(f"Server {hostname} provisioned")


def cmd_delete(args):
    for runner_id in args.ids:
        hostname = f"rise-riscv-runner-{runner_id}"

        print(f"\n{'='*60}")
        print(f"Deleting runner {hostname}")
        print(f"{'='*60}")

        server_id = find_server_by_name(hostname)
        print(f"Found server: {server_id}")

        drain_and_delete_k8s_node(hostname)
        print("Drained and removed from Kubernetes cluster")

        delete_server(server_id)
        print(f"Server {hostname} deleted")


def main():
    parser = argparse.ArgumentParser(description="Provision RISE RISC-V runners on Scaleway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create new runners")
    create_parser.add_argument("count", nargs="?", type=int, default=1, help="Number of new runners to create")

    reinstall_parser = subparsers.add_parser("reinstall", help="Reinstall OS on existing runners")
    reinstall_parser.add_argument("ids", nargs="+", type=int, help="Runner IDs to reinstall (e.g. 1 2 3)")

    delete_parser = subparsers.add_parser("delete", help="Delete existing runners")
    delete_parser.add_argument("ids", nargs="+", type=int, help="Runner IDs to delete (e.g. 1 2 3)")

    args = parser.parse_args()

    if args.command == "create":
        cmd_create(args)
    elif args.command == "reinstall":
        cmd_reinstall(args)
    elif args.command == "delete":
        cmd_delete(args)


if __name__ == "__main__":
    main()
