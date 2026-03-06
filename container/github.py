import functools
import logging
import time

import jwt
import requests

from constants import GHAPP_ID, GHAPP_PRIVATE_KEY

logger = logging.getLogger(__name__)


class GitHubAPIError(Exception):
    """Exception raised for GitHub API errors."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


@functools.lru_cache(maxsize=1)
def init_ghapp_private_key():
    private_key = jwt.jwk_from_pem(GHAPP_PRIVATE_KEY.encode('utf-8'))
    assert private_key, "Failed to load private key from GHAPP_PRIVATE_KEY"
    return private_key


def generate_jwt(app_id, private_key):
    """Generate a JWT for GitHub App authentication."""
    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + (10 * 60),
        "iss": app_id,
    }
    return jwt.JWT().encode(payload, private_key, alg="RS256")


def authenticate_app(installation_id, repo_id=None):
    """Authenticate the app as the organization and get an installation token."""
    jwt_token = generate_jwt(GHAPP_ID, init_ghapp_private_key())

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    body = {}
    if repo_id:
        body["repository_ids"] = [repo_id]
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        logger.debug("Obtained installation access token for installation %s", installation_id)
        return response.json().get("token")
    else:
        error = response.json().get("message")
        logger.error("Failed to get installation access token for installation %s: %s", installation_id, error)
        raise GitHubAPIError(response.status_code, f"Failed to get installation access token: {error}")


def ensure_runner_group(org_name, token, group_name):
    """Ensure the runner group exists and return its ID."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    list_url = f"https://api.github.com/orgs/{org_name}/actions/runner-groups"
    response = requests.get(list_url, headers=headers)
    if response.status_code != 200:
        error = response.json()
        logger.error("Failed to list runner groups for org %s: %s", org_name, error)
        raise GitHubAPIError(response.status_code, f"Failed to list runner groups: {error}")

    for group in response.json().get("runner_groups", []):
        if group.get("name") == group_name:
            logger.debug("Found existing runner group '%s' (id=%s) for org %s",
                        group_name, group["id"], org_name)
            return group["id"]

    create_body = {
        "name": group_name,
        "visibility": "all",
        "allows_public_repositories": True,
    }
    response = requests.post(list_url, headers=headers, json=create_body)
    if response.status_code == 201:
        runner_group_id = response.json().get("id")
        logger.debug("Created runner group '%s' (id=%s) for org %s",
                     group_name, runner_group_id, org_name)
        return runner_group_id
    else:
        error = response.json()
        logger.error("Failed to create runner group '%s' for org %s: %s",
                     group_name, org_name, error)
        raise GitHubAPIError(response.status_code, f"Failed to create runner group: {error}")


def create_jit_runner_config(token, group_id, labels, org_name, runner_name):
    """Create a JIT runner configuration for a new ephemeral runner."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/orgs/{org_name}/actions/runners/generate-jitconfig"
    body = {
        "name": runner_name,
        "runner_group_id": group_id,
        "labels": labels,
    }
    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 201:
        jit_config = response.json().get("encoded_jit_config")
        logger.debug("Created JIT runner config for org %s, runner name=%s", org_name, runner_name)
        return jit_config
    else:
        error = response.json()
        logger.error("Failed to create JIT runner config for org %s: %s", org_name, error)
        raise GitHubAPIError(response.status_code, f"Failed to create JIT runner config: {error}")


def get_job_status(repo_full_name, job_id, token):
    """Get the status of a workflow job from GitHub API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/repos/{repo_full_name}/actions/jobs/{job_id}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data.get("status")  # queued, in_progress, completed
    else:
        logger.error("Failed to get job status for %s job %s: %s", repo_full_name, job_id, response.status_code)
        raise GitHubAPIError(response.status_code, f"Failed to get job status: {response.text}")
