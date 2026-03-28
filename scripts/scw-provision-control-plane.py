#!/usr/bin/env python3
"""Provision RISE RISC-V runner control plane on Scaleway."""

import argparse
import os
import re
import sys

if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import *
from constants import *

SERVER_TYPE = "POP2-2C-8G"
BLOCK_STORAGE_SIZE = 50 * 1_000_000_000

CLOUD_INIT = r"""#cloud-config
write_files:
  - path: /etc/modules-load.d/k8s.conf
    owner: root:root
    permissions: "0644"
    content: |
      overlay
      br_netfilter

  - path: /etc/sysctl.d/k8s.conf
    owner: root:root
    permissions: "0644"
    content: |
      net.bridge.bridge-nf-call-iptables  = 1
      net.bridge.bridge-nf-call-ip6tables = 1
      net.ipv4.ip_forward                 = 1

  - path: /etc/kubernetes/clusterroles.yml
    owner: root:root
    permissions: "0644"
    content: |
      apiVersion: v1
      # luhenry
      items:
      - apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: luhenry-admin-binding
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: cluster-admin
        subjects:
        - apiGroup: rbac.authorization.k8s.io
          kind: User
          name: luhenry
      # gh-app
      - apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: gh-app-edit-binding
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: edit
        subjects:
        - apiGroup: rbac.authorization.k8s.io
          kind: User
          name: gh-app
      - apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRole
        metadata:
          name: gh-app-node-reader
        rules:
        - apiGroups:
          - ""
          resources:
          - nodes
          verbs:
          - list
      - apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: gh-app-node-reader-binding
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: gh-app-node-reader
        subjects:
        - apiGroup: rbac.authorization.k8s.io
          kind: User
          name: gh-app
      kind: List

package_update: true
package_upgrade: true

packages:
  - containerd
  - apt-transport-https
  - ca-certificates
  - curl
  - gpg
  - jq
  - socat
  - conntrack
  - ebtables
  - ipset

bootcmd:
  - modprobe overlay
  - modprobe br_netfilter

runcmd:
  - sysctl --system

  # Configure containerd
  - |
    set -eux
    mkdir -p /etc/containerd
    containerd config default > /etc/containerd/config.toml
    sed -i 's/SystemdCgroup = false/SystemdCgroup = true/g' /etc/containerd/config.toml
    sed -i 's|sandbox_image = ".*"|sandbox_image = "cloudv10x/pause:3.10"|' /etc/containerd/config.toml
    systemctl restart containerd

  # Install kubelet, kubeadm, kubectl from official apt repo
  - |
    set -eux
    curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.35/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
    echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.35/deb/ /' > /etc/apt/sources.list.d/kubernetes.list
    apt-get update
    apt-get install -y kubelet kubeadm kubectl
    apt-mark hold kubelet kubeadm kubectl

  # Initialize the cluster on the private network IP
  - |
    set -eux

    # Discover IPs from the Scaleway metadata service
    METADATA=$(curl -s http://169.254.42.42/conf?format=json)
    PUBLIC_IP=$(echo "${METADATA}" | jq -r '.public_ip.address')

    # Get the private NIC MAC address, find the matching interface, extract its IP
    PRIVATE_MAC=$(echo "${METADATA}" | jq -r '.private_nics[0].mac_address')
    PRIVATE_IFACE=$(ip -o link | grep "${PRIVATE_MAC}" | awk -F': ' '{print $2}')
    PRIVATE_IP=$(ip -4 addr show "${PRIVATE_IFACE}" | grep -oP '(?<=inet\s)[\d.]+' | head -1)

    echo "Public IP:  ${PUBLIC_IP}"
    echo "Private IP: ${PRIVATE_IP}"

    kubeadm init \
      --pod-network-cidr=10.244.0.0/16 \
      --apiserver-advertise-address="${PRIVATE_IP}" \
      --apiserver-cert-extra-sans="${PUBLIC_IP}"

    export KUBECONFIG=/etc/kubernetes/admin.conf

    # Deploy Flannel CNI
    kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml

    # Switch kube-proxy to the multi-arch compatible image
    kubectl set image daemonset/kube-proxy -n kube-system kube-proxy=cloudv10x/kube-proxy:1.35.0

    # Create user kubeconfigs (these will use the private IP as server address;
    # the script replaces it with the public IP when printing)
    kubeadm kubeconfig user --client-name=luhenry > /etc/kubernetes/kubeconfig-luhenry.conf
    kubeadm kubeconfig user --client-name=gh-app > /etc/kubernetes/kubeconfig-gh-app.conf

    # Apply cluster roles
    kubectl apply -f /etc/kubernetes/clusterroles.yml

    # Apply device plugin DaemonSets
    kubectl apply -f https://raw.githubusercontent.com/riseproject-dev/riscv-runner-device-plugin/refs/heads/@@DEVICE_PLUGIN_BRANCH@@/k8s-ds-device-plugin.yaml
    kubectl apply -f https://raw.githubusercontent.com/riseproject-dev/riscv-runner-device-plugin/refs/heads/@@DEVICE_PLUGIN_BRANCH@@/k8s-ds-node-labeller.yaml
"""


def get_next_instance_index(staging):
    resp = instance_api.list_servers()
    prefix = "riscv-runner-control-plane-" + ("staging-" if staging else "")
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+)$")
    used = set()
    for server in resp.servers:
        m = pattern.match(server.name or "")
        if m:
            used.add(int(m.group(1)))
    i = 0
    while i in used:
        i += 1
    return i


def cmd_create(args):
    staging = args.staging
    index = get_next_instance_index(staging)
    hostname = f"riscv-runner-control-plane%s-{index}" % ("-staging" if staging else "")

    print(f"\n{'='*60}")
    print(f"Creating control plane {hostname}")
    print(f"{'='*60}")

    cloud_init = CLOUD_INIT.replace("@@DEVICE_PLUGIN_BRANCH@@", "staging" if staging else "main")

    server = Instance.create(hostname, SERVER_TYPE, BLOCK_STORAGE_SIZE, cloud_init)
    print(f"Server created: {server.id}")

    # Attach to private network (cloud-init discovers the IP at runtime)
    server.attach_private_network()
    print("Private network attached")

    public_ip = server.get_public_ip()
    print(f"Public IP: {public_ip}")

    ssh = ssh_connect(host=public_ip, user="root")

    print("Waiting for cloud-init to complete...")
    ssh.run("cloud-init status --wait", hide=False)

    # Get the private IP that cloud-init discovered
    result = ssh.run("ip -4 addr show scope global", hide=True)
    all_ips = re.findall(r'inet ([\d.]+)', result.stdout)
    private_ip = next((ip for ip in all_ips if ip != public_ip), None)

    # Fetch and print kubeconfigs, replacing private IP with public IP
    # so they're usable from outside the private network
    print(f"\n{'='*60}")
    print("Kubeconfig for luhenry:")
    print(f"{'='*60}")
    result = ssh.run("cat /etc/kubernetes/kubeconfig-luhenry.conf", hide=True)
    if private_ip:
        print(result.stdout.replace(private_ip, public_ip))
    else:
        print(result.stdout)

    print(f"\n{'='*60}")
    print("Kubeconfig for gh-app:")
    print(f"{'='*60}")
    result = ssh.run("cat /etc/kubernetes/kubeconfig-gh-app.conf", hide=True)
    if private_ip:
        print(result.stdout.replace(private_ip, public_ip))
    else:
        print(result.stdout)

    print(f"\n{'='*60}")
    print(f"Control plane {hostname} provisioned successfully")
    print(f"Public IP:  {public_ip}")
    if private_ip:
        print(f"Private IP: {private_ip}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Provision RISE RISC-V runner control plane on Scaleway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create a new control plane")
    create_parser.add_argument("--staging", action="store_true", help="Create a staging control plane")

    args = parser.parse_args()

    if args.command == "create":
        cmd_create(args)
    else:
        print(f"unknown command {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
