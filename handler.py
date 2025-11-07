import hashlib
import hmac
import json
import jwt
import kubernetes as k8s
import os
import requests
import sys
import tempfile
import time

from flask import Flask, request, make_response
app = Flask(__name__)

# --- 3. Authorize the User (Access Control) ---
# This is the allowlist of GitHub organization IDs that are authorized to use this runner.
# Replace these with the actual organization IDs you want to allow.
ALLOWED_ORGS = {
    152654596, # riseproject-dev
}

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

def get_installation_access_token(jwt_token, installation_id):
    """Get an installation access token from GitHub."""
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)

    if response.status_code == 201:
        return response.json().get("token"), None
    else:
        return None, response.json().get("message", "Failed to get installation token")


def check_webhook_signature(headers, body):
    """Verify the webhook signature."""
    secret = os.environ.get("GITHUB_WEBHOOK_SECRET")
    if not secret:
        return None, {"statusCode": 500, "body": "GITHUB_WEBHOOK_SECRET is not configured."}

    signature = headers.get("X-Hub-Signature-256")
    is_valid, message = verify_signature(body, signature, secret)

    if not is_valid:
        return None, {"statusCode": 401, "body": message}

    return body, None

def check_webhook_event(body):
    """Check if the event is a 'queued' workflow_job."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None, {"statusCode": 400, "body": "Invalid JSON payload"}

    if payload.get("action") != "queued":
        return None, {
            "statusCode": 200,
            "body": f"Ignoring action: {payload.get('action')}",
        }

    return payload, None

def authorize_organization(payload):
    """Authorize the organization."""
    org_id = payload.get("organization", {}).get("id")
    if not org_id:
        return None, {"statusCode": 400, "body": "Missing organization ID in payload"}

    if org_id not in ALLOWED_ORGS:
        return None, {
            "statusCode": 200,
            "body": f"Organization {org_id} not authorized.",
        }

    return org_id, None

def authenticate_app_as_organization(payload):
    """Authenticate the app as the organization and get an installation token."""

    private_key = os.environ.get("GITHUB_APP_PRIVATE_KEY")
    if not private_key:
        return None, {
            "statusCode": 500,
            "body": "GITHUB_APP_PRIVATE_KEY is not configured.",
        }

    app_id = 2167633 # https://github.com/apps/rise-risc-v-runner
    private_key = jwt.jwk_from_pem(private_key.encode('utf-8'))

    if not private_key:
        return None, {
            "statusCode": 500,
            "body": "GITHUB_APP_PRIVATE_KEY is not a valid PEM file.",
        }

    installation_id = payload.get("installation", {}).get("id")
    if not installation_id:
        return None, {"statusCode": 400, "body": "Missing installation ID in payload"}

    jwt_token = generate_jwt(app_id, private_key)
    token, error = get_installation_access_token(jwt_token, installation_id)

    if error:
        return None, {"statusCode": 500, "body": error}

    return token, None

def create_runner_registration_token(payload, installation_token):
    """Create a registration token for a new runner."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        return {"statusCode": 400, "body": "Missing organization login in payload"}

    headers = {
        "Authorization": f"token {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/orgs/{org_login}/actions/runners/registration-token"
    response = requests.post(url, headers=headers)

    if response.status_code == 201:
        return response.json().get("token"), None
    else:
        return None, response.json().get("message", "Failed to create runner registration token")

def load_k8s_config():
    """Load Kubernetes configuration."""
    # If not in a cluster, fall back to manual configuration from environment variables.
    # This is an alternative to k8s.config.load_kube_config() which relies on a local file.
    host = os.environ.get("K8S_API_SERVER")
    token = os.environ.get("K8S_API_TOKEN")

    if not host or not token:
        raise k8s.config.ConfigException("K8s not configured: K8S_API_SERVER and K8S_API_TOKEN must be set when not running in-cluster.")

    configuration = k8s.client.Configuration()
    configuration.host = host
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration.api_key['authorization'] = token
    # In a real-world scenario, you would also handle TLS verification,
    # possibly by loading a CA certificate from another env var.
    # For simplicity here, we'll disable it, but this is not recommended for production.
    configuration.verify_ssl = False

    return configuration

def provision_runner(payload, runner_token):
    """Provision a new runner in a Kubernetes pod."""
    with k8s.client.ApiClient(load_k8s_config()) as client:
        api = k8s.client.CoreV1Api(client)

        repo_url = payload.get("repository", {}).get("html_url")
        if not repo_url:
            raise Exception("Missing repository URL in payload")

        pod_name = f"rise-riscv-runner-{payload.get('workflow_job', {}).get('id')}-{int(time.time())}"
        namespace = os.environ.get("K8S_NAMESPACE", "default")
        image = os.environ.get("RUNNER_IMAGE", "riscv64/debian@sha256:5d2913fff700bf597c720fa1385d13c858e536c211955acc59fa747952fc2cef") # Replace with your runner image

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name},
            "spec": {
                "containers": [{
                    "name": "runner",
                    "image": image,
                    "command": ["/bin/bash", "-c"],
                    "args": [
                        f"./config.sh --url {repo_url} --token {runner_token} --name {pod_name} --unattended --ephemeral && ./run.sh"
                    ]
                }],
                "restartPolicy": "Never"
            }
        }

        api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
        return f"Pod {pod_name} created successfully.", None

@app.route("/", methods=['POST'])
def webhook():
    body, err = check_webhook_signature(request.headers, request.get_data(as_text=True))
    if err:
        return make_response(err["body"], err["statusCode"])

    payload, err = check_webhook_event(body)
    if err:
        return make_response(err["body"], err["statusCode"])

    _, err = authorize_organization(payload)
    if err:
        return make_response(err["body"], err["statusCode"])

    installation_token, err = authenticate_app_as_organization(payload)
    if err:
        return make_response(err["body"], err["statusCode"])

    runner_token, err = create_runner_registration_token(payload, installation_token)
    if err:
        return make_response(err["body"], err["statusCode"])

    result, err = provision_runner(payload, runner_token)
    if err:
        return make_response(err["body"], err["statusCode"])

    return result
