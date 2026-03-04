import functools
import hashlib
import hmac
import json
import logging
import jwt
import kubernetes as k8s
import os
import requests
import sys
import time
import yaml

from flask import Flask, request, make_response
app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

class WebhookError(Exception):
    """Exception raised during webhook processing."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)

@app.errorhandler(WebhookError)
def handle_webhook_error(e):
    return make_response(e.message, e.status_code)

# --- 3. Authorize the User (Access Control) ---
# This is the allowlist of GitHub organization IDs that are authorized to use this runner.
# Replace these with the actual organization IDs you want to allow.
ALLOWED_ORGS = {
    152654596, # riseproject-dev
}

VALID_JOB_LABELS = {"rise", "ubuntu-24.04-riscv"}

RUNNER_GROUP_NAME = "RISE RISC-V Runners"

def compute_signature(body, secret):
    return hmac.new(secret.encode('utf-8'), msg=body.encode('utf-8'), digestmod=hashlib.sha256)

def verify_signature(body, signature, secret):
    """Verify that the body was sent from GitHub by validating the signature."""
    if not signature:
        return False, "X-Hub-Signature-256 header is missing!"

    hash = compute_signature(body, secret)
    expected_signature = "sha256=" + hash.hexdigest()

    if not hmac.compare_digest(expected_signature, signature):
        return False, f"Request signatures didn't match! Expected: {expected_signature}, Got: {signature}"

    return True, "Signatures match"

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
        logger.info("Obtained installation access token for installation %s, response = %s", installation_id, response.json())
        return response.json().get("token")
    else:
        error = response.json().get("message", "Failed to get installation token")
        logger.error("Failed to get installation access token for installation %s: %s", installation_id, error)
        raise WebhookError(500, error)


def check_webhook_signature(headers, body):
    """Verify the webhook signature."""
    secret = os.environ.get("GHAPP_WEBHOOK_SECRET")
    if not secret:
        raise WebhookError(500, "GHAPP_WEBHOOK_SECRET is not configured.")

    signature = headers.get("X-Hub-Signature-256")
    is_valid, message = verify_signature(body, signature, secret)

    if not is_valid:
        logger.warning("Webhook signature verification failed: %s", message)
        raise WebhookError(401, message)

    return body

def check_webhook_event(body):
    """Check if the event is a 'queued' workflow_job."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise WebhookError(400, "Invalid JSON payload")

    action = payload.get("action")
    if action not in ("queued", "completed"):
        logger.info("Ignoring action: %s", action)
        raise WebhookError(200, f"Ignoring action: {action}")

    job = payload.get("workflow_job", {})
    logger.info("Received %s workflow_job id=%s name=%s repo=%s labels=%s",
                action, job.get("id"), job.get("name"),
                payload.get("repository", {}).get("full_name"),
                job.get("labels"))

    return payload, action

def check_required_labels(payload):
    """Check that the workflow job has the required runs-on labels."""
    job_labels = set(payload.get("workflow_job", {}).get("labels", []))

    if any(label not in VALID_JOB_LABELS for label in job_labels):
        logger.info("Ignoring job: contains unsupported labels (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: contains unsupported labels.")

    if not "rise" in job_labels:
        logger.info("Ignoring job: missing required 'rise' label (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: missing required 'rise' label.")

    if "ubuntu-24.04-riscv" in job_labels:
        k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    # elif "ubuntu-26.04-riscv" in job_labels:
    #     k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    else:
        logger.info("Ignoring job: missing required platform label (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: missing required platform label.")

    SCW_EM_RV1_SPEC = {
        "nodeSelector": {
            "riseproject.dev/board": "scw-em-rv1",
        },
    }
    # SCW_EM_RV2_SPEC = {
    #     "nodeSelector": {
    #         "riseproject.dev/board": "scw-em-rv2",
    #     },
    # }

    # Defaults to the Scaleway EM-RV1 board
    k8s_spec = SCW_EM_RV1_SPEC

    # We want to support more labels like "rva23", or "rvv" in the future
    # if "rva23" in job_labels or "rvv" in job_labels::
    #     k8s_spec = SCW_EM_RV2_SPEC

    return k8s_image, k8s_spec, list(job_labels)

def authorize_organization(payload):
    """Authorize the organization."""
    org_id = payload.get("organization", {}).get("id")
    if not org_id:
        raise WebhookError(400, "Missing organization ID in payload")

    if org_id not in ALLOWED_ORGS:
        logger.info("Organization %s (%s) not authorized",
                     payload.get("organization", {}).get("login"), org_id)
        raise WebhookError(200, f"Organization {org_id} not authorized.")

    logger.info("Organization %s authorized", payload.get("organization", {}).get("login"))
    return org_id

def authenticate_app_as_organization(payload):
    """Authenticate the app as the organization and get an installation token."""

    private_key = os.environ.get("GHAPP_PRIVATE_KEY")
    if not private_key:
        raise WebhookError(500, "GHAPP_PRIVATE_KEY is not configured.")

    app_id = 2167633 # https://github.com/apps/rise-risc-v-runner
    private_key = jwt.jwk_from_pem(private_key.encode('utf-8'))

    if not private_key:
        raise WebhookError(500, "GHAPP_PRIVATE_KEY is not a valid PEM file.")

    installation_id = payload.get("installation", {}).get("id")
    if not installation_id:
        raise WebhookError(400, "Missing installation ID in payload")

    repo_id = payload.get("repository", {}).get("id")
    if not repo_id:
        raise WebhookError(400, "Missing repository ID in payload")

    jwt_token = generate_jwt(app_id, private_key)
    return get_installation_access_token(jwt_token, installation_id, repo_id)

def ensure_runner_group(payload, installation_token):
    """Ensure the runner group exists and return its ID."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        raise WebhookError(400, "Missing organization login in payload")

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
        raise WebhookError(500, f"Failed to list runner groups: {error}")

    for group in response.json().get("runner_groups", []):
        if group.get("name") == RUNNER_GROUP_NAME:
            logger.info("Found existing runner group '%s' (id=%s) for org %s",
                        RUNNER_GROUP_NAME, group["id"], org_login)
            return group["id"]

    # Group not found, create it
    create_body = {
        "name": RUNNER_GROUP_NAME,
        "visibility": "all",
        "allows_public_repositories": True,
    }
    response = requests.post(list_url, headers=headers, json=create_body)
    if response.status_code == 201:
        group_id = response.json().get("id")
        logger.info("Created runner group '%s' (id=%s) for org %s",
                     RUNNER_GROUP_NAME, group_id, org_login)
        return group_id
    else:
        error = response.json()
        logger.error("Failed to create runner group '%s' for org %s: %s",
                     RUNNER_GROUP_NAME, org_login, error)
        raise WebhookError(500, f"Failed to create runner group: {error}")

def create_jit_runner_config(payload, installation_token, runner_group_id, job_labels):
    """Create a JIT runner configuration for a new ephemeral runner."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        raise WebhookError(400, "Missing organization login in payload")

    job_id = payload.get("workflow_job", {}).get("id")
    if not job_id:
        raise WebhookError(400, "Missing workflow_job id in payload")

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
        logger.info("Created JIT runner config for org %s, runner name=%s, group_id=%s",
                     org_login, pod_name, runner_group_id)
        return jit_config, pod_name
    else:
        error = response.json()
        logger.error("Failed to create JIT runner config for org %s: %s", org_login, error)
        raise WebhookError(500, f"Failed to create JIT runner config: {error}")

@functools.lru_cache(maxsize=1)
def init_k8s_config():
    """Load Kubernetes configuration from a kubeconfig env var.
    Called once at startup; result is memoized."""
    kubeconfig = os.environ.get("K8S_KUBECONFIG")
    if not kubeconfig:
        raise k8s.config.ConfigException(
            "K8s not configured: K8S_KUBECONFIG must be set to a kubeconfig."
        )
    return yaml.safe_load(kubeconfig)

def provision_runner(payload, jit_config, pod_name, k8s_image, k8s_spec):
    """Provision a new runner in a Kubernetes pod."""
    with k8s.config.new_client_from_config_dict(init_k8s_config()) as client:
        api = k8s.client.CoreV1Api(client)

        image = os.environ.get("RUNNER_IMAGE", k8s_image)

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "labels": {"app": "rise-riscv-runner"}},
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

        api.create_namespaced_pod(body=pod_manifest, namespace="default")
        repo = payload.get("repository", {}).get("full_name")
        job_url = payload.get("workflow_job", {}).get("html_url")
        logger.info("Provisioned runner pod %s for repo=%s, image=%s, job=%s", pod_name, repo, image, job_url)
        return f"Pod {pod_name} created successfully."

def delete_runner(payload):
    """Delete a runner pod for a cancelled workflow job."""
    job_id = payload.get("workflow_job", {}).get("id")
    if not job_id:
        raise WebhookError(400, "Missing workflow_job id in payload")

    pod_name = f"rise-riscv-runner-workflow-{job_id}"

    with k8s.config.new_client_from_config_dict(init_k8s_config()) as client:
        api = k8s.client.CoreV1Api(client)
        try:
            api.delete_namespaced_pod(name=pod_name, namespace="default")
            repo = payload.get("repository", {}).get("full_name")
            job_url = payload.get("workflow_job", {}).get("html_url")
            logger.info("Deleted runner pod %s for repo=%s, job=%s", pod_name, repo, job_url)
            return f"Pod {pod_name} deleted successfully."
        except k8s.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.info("Pod %s not found, already deleted", pod_name)
                return f"Pod {pod_name} not found."
            raise

@app.route("/health", methods=['GET'])
def health():
    return "ok"

@app.route("/", methods=['POST'])
def webhook():
    body = check_webhook_signature(request.headers, request.get_data(as_text=True))
    payload, action = check_webhook_event(body)

    if action == "queued":
        k8s_image, k8s_spec, job_labels = check_required_labels(payload)
        authorize_organization(payload)
        installation_token = authenticate_app_as_organization(payload)
        runner_group_id = ensure_runner_group(payload, installation_token)
        jit_config, pod_name = create_jit_runner_config(payload, installation_token, runner_group_id, job_labels)
        return provision_runner(payload, jit_config, pod_name, k8s_image, k8s_spec)
    elif action == "completed":
        return delete_runner(payload)
    else:
        logger.info("Ignoring {action} job")
        raise WebhookError(200, f"Ignoring {action} job")

