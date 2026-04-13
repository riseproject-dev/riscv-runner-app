import hashlib
import hmac
import json
import logging
import requests

from flask import Flask, request, make_response

import db
from constants import *

app = Flask(__name__)

logger = logging.getLogger(__name__)


class WebhookError(Exception):
    """Exception raised during webhook processing."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


@app.errorhandler(WebhookError)
def handle_webhook_error(e):
    if e.status_code == 200:
        logger.debug(e.message)
    else:
        logger.warning(e.message)
    return make_response(e.message, e.status_code)


@app.errorhandler(AssertionError)
def handle_assertion_error(e):
    logger.info(e)
    return make_response(str(e), 400)


@app.after_request
def log_request(response):
    if request.method == "GET" and request.path == "/health":
        pass
    elif response.status_code == 200:
        logger.debug("%s %s %s", request.method, request.path, response.status_code)
    else:
        logger.info("%s %s %s", request.method, request.path, response.status_code)
    return response


# --- Staging Proxy ---

@app.before_request
def proxy_to_staging():
    if not PROD:
        return

    if request.method != "POST" or request.path != "/":
        return

    if request.headers.get("X-Github-Event") != "workflow_job":
        # only redirect workflow_job events
        return

    body = request.get_data(as_text=True)
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return

    entity_id = payload["repository"]["owner"]["id"]
    if entity_id not in STAGING_ENTITIES:
        logger.debug("Received request for entity=%s, not in staging entities, skipping proxy", entity_id)
        return

    repo_name = payload["repository"]["name"]
    if repo_name not in STAGING_ENTITIES[entity_id]:
        logger.debug("Received request for entity=%s repo=%s, not in staging entities, skipping proxy", entity_id, repo_name)
        return

    logger.debug("Proxying request for entity=%s repo=%s to staging (%s)", entity_id, repo_name, STAGING_URL)
    resp = requests.post(
        STAGING_URL,
        data=request.get_data(),
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        timeout=30,
    )
    logger.info("Proxied request for entity=%s repo=%s to staging, status=%s", entity_id, repo_name, resp.status_code)
    return make_response(resp.content, resp.status_code)


# --- Webhook validation ---

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


def check_webhook_signature(headers, body):
    """Verify the webhook signature."""
    if not "X-Github-Event" in request.headers:
        raise WebhookError(400, "Missing X-Github-Event header")
    event = headers["X-Github-Event"]

    if not "X-Hub-Signature-256" in request.headers:
        raise WebhookError(400, "Missing X-Hub-Signature-256 header")
    signature = headers["X-Hub-Signature-256"]

    is_valid, message = verify_signature(body, signature, GHAPP_WEBHOOK_SECRET)
    if not is_valid:
        logger.warning("Webhook signature verification failed: %s", message)
        raise WebhookError(401, message)

    return event, body


def check_webhook_event(body):
    """Check if the event is a workflow_job with a handled action."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.debug("Invalid JSON payload")
        raise WebhookError(400, "Invalid JSON payload")

    action = payload["action"]
    if action not in ("queued", "in_progress", "completed"):
        logger.debug("Ignoring action: %s", action)
        raise WebhookError(200, f"Ignoring action: {action}")

    return payload, action


def authorize_entity(payload):
    """Authorize the repository owner (organization or personal account)."""
    owner = payload["repository"]["owner"]
    owner_id = owner["id"]
    if not owner_id:
        raise WebhookError(400, "Owner ID is missing in payload")

    owner_type = owner["type"]
    if not owner_type:
        raise WebhookError(400, "Owner Type is missing in payload")
    if owner_type not in (EntityType.ORGANIZATION, EntityType.USER):
        raise WebhookError(400, f"Unsupported owner type: {owner_type}")

    entity_type = EntityType(owner_type)

    return owner_id, entity_type


def match_labels_to_k8s(org_id, repo_full_name, job_labels):
    """
    Map workflow job labels to a k8s pool name and container image.

    Returns (k8s_pool, k8s_image) where k8s_pool is the board name string
    used as k8s pool key and pod label.
    """
    # Special case(s) for PyTorch org
    if org_id == PYTORCH_ORG_ID or (org_id == RISEPROJECT_DEV_ORG_ID and repo_full_name in ["riseproject-dev/pytorch", "riseproject-dev/executorch"]):
        if any("linux.riscv64.xlarge" in job_label or "linux.riscv64.2xlarge" in job_label for job_label in job_labels):
            return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_24_04
        elif "ubuntu-24.04-riscv" in job_labels:
            return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_24_04
        else:
            raise WebhookError(200, f"Ignoring job: missing required platform label (got {job_labels}) for PyTorch org")

    # Special case(s) for GGML org
    elif org_id == GGML_ORG_ORG_ID or (org_id == RISEPROJECT_DEV_ORG_ID and repo_full_name.endswith("/llama.cpp")):
        if job_labels == ["ubuntu-24.04-riscv"]:
            return "cloudv10x-jupiter", RUNNER_IMAGE_UBUNTU_24_04
        else:
            raise WebhookError(200, f"Ignoring job: missing required platform label (got {job_labels}) for GGML org")

    # General cases
    elif job_labels == ["ubuntu-24.04-riscv"]:
        return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_24_04
    # FIXME: there is no hardware that supports 26.04 (RVA23) just yet
    # elif job_labels == ["ubuntu-26.04-riscv"]:
    #     return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_26_04

    raise WebhookError(200, f"Ignoring job: missing required platform label (got {job_labels})")


# --- Routes ---

@app.route("/health", methods=['GET'])
def health():
    return "ok"


@app.route("/", methods=['POST'])
def webhook():
    event, body = check_webhook_signature(request.headers, request.get_data(as_text=True))

    if event == "ping":
        return f"pong"

    elif event == "workflow_job":
        payload, action = check_webhook_event(body)

        owner_id, entity_type = authorize_entity(payload)

        job_id = payload["workflow_job"]["id"]
        if not job_id:
            raise WebhookError(400, "Job ID is missing in payload")

        # labels may be missing when no labels are defined
        job_labels = payload["workflow_job"]["labels"] or []

        repo_full_name = payload["repository"]["full_name"]
        if not repo_full_name:
            raise WebhookError(400, "Repository full name is missing in payload")

        repo_id = payload["repository"]["id"]
        if not repo_id:
            raise WebhookError(400, "Repository ID is missing in payload")

        # entity_id: owner_id (org) for organizations, repo_id for personal accounts
        entity_id = owner_id if entity_type == EntityType.ORGANIZATION else repo_id

        # Make sure the required labels are present; Filters out unsupported jobs early
        k8s_pool, k8s_image = match_labels_to_k8s(owner_id, repo_full_name, job_labels)

        logger.info("Received %s workflow_job id=%s name=%s repo=%s labels=%s entity_type=%s",
                    action, job_id, payload["workflow_job"]["name"],
                    payload["repository"]["full_name"],
                    payload["workflow_job"]["labels"],
                    entity_type.value)

        if action == "queued":
            installation_id = payload["installation"]["id"]
            if not installation_id:
                raise WebhookError(400, "Installation ID is missing in payload")

            entity_name = payload["repository"]["owner"]["login"]
            if not entity_name:
                raise WebhookError(400, "Entity name is missing in payload")

            html_url = payload["workflow_job"]["html_url"]
            if not html_url:
                raise WebhookError(400, "HTML URL is missing in payload")

            stored = db.store_job(
                job_id=job_id,
                entity_id=entity_id,
                entity_name=entity_name,
                entity_type=entity_type,
                repo_full_name=repo_full_name,
                installation_id=installation_id,
                labels=job_labels,
                k8s_pool=k8s_pool,
                k8s_image=k8s_image,
                html_url=html_url,
            )

            if stored:
                return f"Job {job_id} stored."
            else:
                return f"Job {job_id} already exists."

        elif action == "in_progress":
            prev_status = db.update_job_running(job_id)
            if prev_status is None:
                logger.warning("Job %s not found on in_progress event", job_id)
                return f"Job {job_id} not found."
            logger.info("Job %s marked running (was %s)", job_id, prev_status)
            return f"Job {job_id} marked running (was {prev_status})."

        elif action == "completed":
            prev_status = db.update_job_completed(job_id)
            if prev_status is None:
                logger.warning("Job %s not found on completed event", job_id)
                return f"Job {job_id} not found."
            return f"Job {job_id} completed (was {prev_status})."

        else:
            return f"Ignoring {action} job"

    else:
        return f"Ignoring {event} event"

if __name__ == "__main__":
    # Set the logging level for all loggers to INFO
    logging.basicConfig(
        level=logging.INFO,
        format='%(pathname)s:%(lineno)d::%(funcName)s: [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Ensure PostgreSQL schema/tables exist
    db.ensure_schema()

    from waitress import serve

    HOST = "0.0.0.0"
    PORT = 8080

    print(f"Starting server on http://{HOST}:{PORT}")
    serve(app, host=HOST, port=PORT)
