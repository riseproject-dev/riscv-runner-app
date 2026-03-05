import functools
import json
import logging
import jwt
import kubernetes as k8s
import os
import requests
import time
import yaml

logger = logging.getLogger(__name__)


from constants import *


class RunnerError(Exception):
    """Exception raised during runner provisioning."""
    def __init__(self, message):
        self.message = message
        super().__init__(message)


@functools.lru_cache(maxsize=1)
def init_k8s_client():
    """Create a Kubernetes API client from a kubeconfig env var.
    Called once at startup; result is memoized."""
    kubeconfig = os.environ.get("K8S_KUBECONFIG")
    if not kubeconfig:
        raise k8s.config.ConfigException(
            "K8s not configured: K8S_KUBECONFIG must be set to a kubeconfig."
        )
    return k8s.config.new_client_from_config_dict(yaml.safe_load(kubeconfig))


def generate_jwt(app_id, private_key):
    """Generate a JWT for GitHub App authentication."""
    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + (10 * 60),  # 10 minutes expiration
        "iss": app_id,
    }
    return jwt.JWT().encode(payload, private_key, alg="RS256")


@functools.lru_cache(maxsize=1)
def init_ghapp_private_key():
    private_key = jwt.jwk_from_pem(GHAPP_PRIVATE_KEY.encode('utf-8'))
    assert private_key, "Failed to load private key from GHAPP_PRIVATE_KEY"

    return private_key


def authenticate_app(installation_id, repo_id):
    """Authenticate the app as the organization and get an installation token."""

    jwt_token = generate_jwt(GHAPP_ID, init_ghapp_private_key())

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    body = {"repository_ids": [repo_id]}
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        logger.debug("Obtained installation access token for installation %s, response = %s", installation_id, response.json())
        return response.json().get("token")
    else:
        error = response.json().get("message", "Failed to get installation token")
        logger.error("Failed to get installation access token for installation %s: %s", installation_id, error)
        raise RunnerError(error)



def ensure_runner_group_on_org(org_name, installation_token, runner_group_name):
    """Ensure the runner group exists and return its ID."""

    headers = {
        "Authorization": f"Bearer {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # List existing runner groups
    list_url = f"https://api.github.com/orgs/{org_name}/actions/runner-groups"
    response = requests.get(list_url, headers=headers)
    if response.status_code != 200:
        error = response.json()
        logger.error("Failed to list runner groups for org %s: %s", org_name, error)
        raise RunnerError(f"Failed to list runner groups: {error}")

    for group in response.json().get("runner_groups", []):
        if group.get("name") == runner_group_name:
            logger.debug("Found existing runner group '%s' (id=%s) for org %s",
                        runner_group_name, group["id"], org_name)
            return group["id"]

    # Group not found, create it
    create_body = {
        "name": runner_group_name,
        "visibility": "all",
        "allows_public_repositories": True,
    }
    response = requests.post(list_url, headers=headers, json=create_body)
    if response.status_code == 201:
        runner_group_id = response.json().get("id")
        logger.debug("Created runner group '%s' (id=%s) for org %s",
                     runner_group_name, runner_group_id, org_name)
        return runner_group_id
    else:
        error = response.json()
        logger.error("Failed to create runner group '%s' for org %s: %s",
                     runner_group_name, org_name, error)
        raise RunnerError(f"Failed to create runner group: {error}")


def create_jit_runner_config_on_org(installation_token, runner_group_id, job_labels, org_name, runner_name):
    """Create a JIT runner configuration for a new ephemeral runner."""

    headers = {
        "Authorization": f"Bearer {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/orgs/{org_name}/actions/runners/generate-jitconfig"
    body = {
        "name": runner_name,
        "runner_group_id": runner_group_id,
        "labels": job_labels,
    }
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        jit_config = response.json().get("encoded_jit_config")
        logger.debug("Created JIT runner config for org %s, runner name=%s, group_id=%s",
                     org_name, runner_name, runner_group_id)
        return jit_config
    else:
        error = response.json()
        logger.error("Failed to create JIT runner config for org %s: %s", org_name, error)
        raise RunnerError(f"Failed to create JIT runner config: {error}")


def provision_runner(jit_config, runner_name, k8s_image, k8s_spec, job_id):
    """Provision a new runner in a Kubernetes pod."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": runner_name,
                "labels": {
                    "app": "rise-riscv-runner",
                    "riseproject.com/job_id": str(job_id),
                },
            },
            "spec": {
                **k8s_spec,
                "containers": [{
                    "name": "runner",
                    "image": k8s_image,
                    "command": ["/bin/bash", "-eux", "-o", "pipefail", "-c"],
                    "args": [
                        f"./run.sh --jitconfig {jit_config}"
                    ],
                    "resources": {
                        "limits": {
                            "riseproject.com/runner": "1",
                        }
                    }
                }],
                "restartPolicy": "Never"
            }
        }

        api.create_namespaced_pod(body=pod_manifest, namespace=K8S_NAMESPACE)


def delete_pod(pod):
    """Delete a runner pod."""
    assert pod, "Pod must be provided to delete it"
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)
        try:
            api.delete_namespaced_pod(name=pod.metadata.name, namespace=K8S_NAMESPACE)
            logger.info("Deleted runner pod %s", pod.metadata.name)
            return f"Pod {pod.metadata.name} deleted successfully."
        except k8s.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.debug("Pod %s not found, already deleted", pod.metadata.name)
                return f"Pod {pod.metadata.name} not found."
            raise


def has_available_slot(node_selector):
    """Check if there's an available runner slot on nodes matching the selector."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)

        nodes = api.list_node()
        matching_nodes = [
            node for node in nodes.items
            if all(node.metadata.labels.get(k) == v for k, v in node_selector.items())
        ]
        total = sum(
            int(node.status.allocatable.get("riseproject.com/runner", "0"))
            for node in matching_nodes
        )

        # Count active pods on matching nodes
        pods = api.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=rise-riscv-runner"
        )
        active = sum(
            1 for p in pods.items
            if p.status.phase in ("Pending", "Running")
            and p.spec.node_selector == node_selector
        )

        available = total - active
        logger.info("Capacity check: node_selector=%s, total=%d, active=%d, available=%d",
                     node_selector, total, active, available)
        return available > 0

def list_pods():
    """Get all runner pods."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)
        pods = api.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=rise-riscv-runner"
        )
        return pods.items

def find_pod_by_job_id(job_id):
    """Find a runner pod by its job_id label. Returns the pod name or None."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)
        pods = api.list_namespaced_pod(
            namespace=K8S_NAMESPACE,
            label_selector=f"app=rise-riscv-runner,riseproject.com/job_id={job_id}",
        )
        return pods.items[0] if pods.items else None