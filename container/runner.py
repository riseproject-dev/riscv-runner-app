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


PROD = os.environ["PROD"].lower() == "true"
PROD_URL = os.environ["PROD_URL"]
STAGING_URL = os.environ["STAGING_URL"]

K8S_NAMESPACE = "default" if PROD else "staging"

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


def get_installation_access_token(jwt_token, installation_id, repository_id):
    """Get an installation access token from GitHub, scoped to a single repository."""
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    body = {"repository_ids": [repository_id]}
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        logger.debug("Obtained installation access token for installation %s, response = %s", installation_id, response.json())
        return response.json().get("token")
    else:
        error = response.json().get("message", "Failed to get installation token")
        logger.error("Failed to get installation access token for installation %s: %s", installation_id, error)
        raise RunnerError(error)


def authenticate_app(payload):
    """Authenticate the app as the organization and get an installation token."""
    private_key = os.environ.get("GHAPP_PRIVATE_KEY")
    if not private_key:
        raise RunnerError("GHAPP_PRIVATE_KEY is not configured.")

    app_id = 2167633  # https://github.com/apps/rise-risc-v-runner
    private_key = jwt.jwk_from_pem(private_key.encode('utf-8'))

    if not private_key:
        raise RunnerError("GHAPP_PRIVATE_KEY is not a valid PEM file.")

    installation_id = payload.get("installation", {}).get("id")
    if not installation_id:
        raise RunnerError("Missing installation ID in payload")

    repo_id = payload.get("repository", {}).get("id")
    if not repo_id:
        raise RunnerError("Missing repository ID in payload")

    jwt_token = generate_jwt(app_id, private_key)
    return get_installation_access_token(jwt_token, installation_id, repo_id)


def ensure_runner_group(payload, installation_token, runner_group_name):
    """Ensure the runner group exists and return its ID."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        raise RunnerError("Missing organization login in payload")

    headers = {
        "Authorization": f"Bearer {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # List existing runner groups
    list_url = f"https://api.github.com/orgs/{org_login}/actions/runner-groups"
    response = requests.get(list_url, headers=headers)
    if response.status_code != 200:
        error = response.json()
        logger.error("Failed to list runner groups for org %s: %s", org_login, error)
        raise RunnerError(f"Failed to list runner groups: {error}")

    for group in response.json().get("runner_groups", []):
        if group.get("name") == runner_group_name:
            logger.debug("Found existing runner group '%s' (id=%s) for org %s",
                        runner_group_name, group["id"], org_login)
            return group["id"]

    # Group not found, create it
    create_body = {
        "name": runner_group_name,
        "visibility": "all",
        "allows_public_repositories": True,
    }
    response = requests.post(list_url, headers=headers, json=create_body)
    if response.status_code == 201:
        group_id = response.json().get("id")
        logger.debug("Created runner group '%s' (id=%s) for org %s",
                     runner_group_name, group_id, org_login)
        return group_id
    else:
        error = response.json()
        logger.error("Failed to create runner group '%s' for org %s: %s",
                     runner_group_name, org_login, error)
        raise RunnerError(f"Failed to create runner group: {error}")


def create_jit_runner_config(payload, installation_token, runner_group_id, job_labels):
    """Create a JIT runner configuration for a new ephemeral runner."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        raise RunnerError("Missing organization login in payload")

    job_id = payload.get("workflow_job", {}).get("id")
    if not job_id:
        raise RunnerError("Missing workflow_job id in payload")

    pod_name = f"rise-riscv-runner-workflow-{job_id}"

    headers = {
        "Authorization": f"Bearer {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/orgs/{org_login}/actions/runners/generate-jitconfig"
    body = {
        "name": pod_name,
        "runner_group_id": runner_group_id,
        "labels": job_labels,
    }
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        jit_config = response.json().get("encoded_jit_config")
        logger.debug("Created JIT runner config for org %s, runner name=%s, group_id=%s",
                     org_login, pod_name, runner_group_id)
        return jit_config, pod_name
    else:
        error = response.json()
        logger.error("Failed to create JIT runner config for org %s: %s", org_login, error)
        raise RunnerError(f"Failed to create JIT runner config: {error}")


def provision_runner(payload, jit_config, pod_name, k8s_image, k8s_spec):
    """Provision a new runner in a Kubernetes pod."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)

        image = os.environ.get("RUNNER_IMAGE", k8s_image)

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "labels": {"app": "rise-riscv-runner"},
                "annotations": {"riseproject.com/job_id": str(payload.get("workflow_job", {}).get("id", ""))},
            },
            "spec": {
                **k8s_spec,
                "containers": [{
                    "name": "runner",
                    "image": image,
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
        repo = payload.get("repository", {}).get("full_name")
        job_url = payload.get("workflow_job", {}).get("html_url")
        logger.info("Provisioned runner pod %s for repo=%s, image=%s, job=%s", pod_name, repo, image, job_url)
        return f"Pod {pod_name} created successfully."


def delete_pod(pod_name):
    """Delete a runner pod by name."""
    with init_k8s_client() as client:
        api = k8s.client.CoreV1Api(client)
        try:
            api.delete_namespaced_pod(name=pod_name, namespace=K8S_NAMESPACE)
            logger.info("Deleted runner pod %s", pod_name)
            return f"Pod {pod_name} deleted successfully."
        except k8s.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.debug("Pod %s not found, already deleted", pod_name)
                return f"Pod {pod_name} not found."
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