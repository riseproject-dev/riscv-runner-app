import hashlib
import hmac
import json
import logging
import os

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

# --- Access Control ---
ALLOWED_ORGS = {
    152654596, # riseproject-dev
}

VALID_JOB_LABELS = {"rise", "ubuntu-24.04-riscv"}

# --- Staging Proxy ---
# Organizations whose webhooks are proxied from production to staging.
STAGING_ORGS = {
    152654596, # riseproject-dev
}

@app.before_request
def proxy_to_staging():
    if request.method != "POST" or request.path != "/":
        return None

    prod_url = os.environ.get("PROD_URL", "")
    staging_url = os.environ.get("STAGING_URL", "")
    if not prod_url or not staging_url:
        return None

    # Only proxy when running as the production instance
    request_url = request.url_root.rstrip("/")
    if request_url != prod_url.rstrip("/"):
        return None

    body = request.get_data(as_text=True)
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return None

    org_id = payload.get("organization", {}).get("id")
    if org_id not in STAGING_ORGS:
        return None

    import requests as req
    resp = req.post(
        staging_url,
        data=request.get_data(),
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        timeout=30,
    )
    logger.info("Proxied request for org %s to staging, status=%s", org_id, resp.status_code)
    return make_response(resp.content, resp.status_code)

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
    """Check if the event is a 'queued' or 'completed' workflow_job."""
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
    # if "rva23" in job_labels or "rvv" in job_labels:
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

@app.route("/health", methods=['GET'])
def health():
    return "ok"

@app.route("/", methods=['POST'])
def webhook():
    import redis_client
    from worker import queue_lock, queue_event
    from runner import delete_pod

    body = check_webhook_signature(request.headers, request.get_data(as_text=True))
    payload, action = check_webhook_event(body)

    if action == "queued":
        k8s_image, k8s_spec, job_labels = check_required_labels(payload)

        # This should be removed at some point as all organizations should be allowed
        authorize_organization(payload)

        job_id = payload.get("workflow_job", {}).get("id")
        if not job_id:
            raise WebhookError(400, "Missing workflow_job id in payload")

        r = redis_client.connect()
        with queue_lock:
            enqueued = redis_client.enqueue_job(r, job_id, payload, k8s_image, k8s_spec, job_labels)
            if enqueued:
                queue_event.notify()

        if enqueued:
            return f"Job {job_id} enqueued."
        else:
            return f"Job {job_id} already enqueued."

    elif action == "completed":
        job_id = payload.get("workflow_job", {}).get("id")
        if not job_id:
            raise WebhookError(400, "Missing workflow_job id in payload")

        r = redis_client.connect()
        with queue_lock:
            prev_status, pod_name = redis_client.complete_job(r, job_id)

        if prev_status is None:
            # Job not in Redis — attempt direct pod deletion as fallback
            pod_name = f"rise-riscv-runner-workflow-{job_id}"
            logger.warning("Job %s not found in Redis, attempting direct pod deletion: %s", job_id, pod_name)
            delete_pod(pod_name)
            return f"Job {job_id} not found, fallback pod deletion attempted."

        return f"Job {job_id} marked completed (was {prev_status})."

    else:
        raise WebhookError(200, f"Ignoring {action} job")
