import hashlib
import hmac
import json
import logging
import requests

from flask import Flask, request, make_response

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
    return make_response(e.message, e.status_code)

@app.after_request
def log_request(response):
    if request.method == "GET" and request.path == "/health":
        pass # skip logging health checks
    elif response.status_code == 200:
        pass # skip logging successful requests to reduce noise
    else:
        logger.info("%s %s %s", request.method, request.path, response.status_code)

    return response

# --- Access Control ---
ALLOWED_ORGS = {
    # Organizations
    152654596, # riseproject-dev
    # Individuals
    660779, # luhenry
}

VALID_JOB_LABELS = {"rise", "ubuntu-24.04-riscv"}

# --- Staging Proxy ---
# Organizations whose webhooks are proxied from production to staging.
STAGING_ORGS = {
    152654596, # riseproject-dev
}

@app.before_request
def proxy_to_staging():
    if not PROD:
        return

    if request.method != "POST" or request.path != "/":
        logger.debug("Proxy skipped: not a POST to / (method=%s, path=%s)", request.method, request.path)
        return

    body = request.get_data(as_text=True)
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        logger.debug("Proxy skipped: invalid JSON payload")
        return

    org_id = payload["repository"]["owner"]["id"]
    if org_id not in STAGING_ORGS:
        logger.debug("Proxy skipped: org %s not in STAGING_ORGS", org_id)
        return

    logger.debug("Proxying request for org %s to staging (%s)", org_id, STAGING_URL)

    resp = requests.post(
        STAGING_URL,
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
    signature = headers.get("X-Hub-Signature-256")
    is_valid, message = verify_signature(body, signature, GHAPP_WEBHOOK_SECRET)

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

    action = payload["action"]
    if action not in ("queued", "completed"):
        logger.debug("Ignoring action: %s", action)
        raise WebhookError(200, f"Ignoring action: {action}")

    job = payload["workflow_job"]
    logger.info("Received %s workflow_job id=%s name=%s repo=%s labels=%s",
                action, job.get("id"), job.get("name"),
                payload["repository"]["full_name"],
                job.get("labels"))

    return payload, action

def check_required_labels(payload):
    """Check that the workflow job has the required runs-on labels."""
    job_labels = set(payload["workflow_job"]["labels"])

    if any(label not in VALID_JOB_LABELS for label in job_labels):
        logger.debug("Ignoring job: contains unsupported labels (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: contains unsupported labels.")

    if not "rise" in job_labels:
        logger.debug("Ignoring job: missing required 'rise' label (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: missing required 'rise' label.")

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
    CLOUDV10X_RVV_SPEC = {
        "nodeSelector": {
            "riseproject.dev/board": "cloudv10x-rvv",
        },
    }

    if "ubuntu-24.04-riscv" in job_labels:
        k8s_spec = SCW_EM_RV1_SPEC
        k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    elif "ubuntu-24.04-riscv-rvv" in job_labels:
        k8s_spec = CLOUDV10X_RVV_SPEC
        k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    # elif "ubuntu-24.04-riscv-rva23" in job_labels:
    #     k8s_spec = SCW_EM_RV2_SPEC
    #     k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    # elif "ubuntu-26.04-riscv" in job_labels:
    #     k8s_spec = SCW_EM_RV1_SPEC
    #     k8s_image = "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    else:
        logger.debug("Ignoring job: missing required platform label (got %s)", sorted(job_labels))
        raise WebhookError(200, "Ignoring job: missing required platform label.")

    return k8s_image, k8s_spec, list(job_labels)

def authorize_organization(payload):
    """Authorize the organization."""
    org_id = payload["repository"]["owner"]["id"]
    if not org_id:
        raise WebhookError(400, "Missing organization ID in payload")

    if org_id not in ALLOWED_ORGS:
        logger.warning("Organization %s (%s) not authorized",
                     payload["repository"]["owner"]["login"], org_id)
        raise WebhookError(200, f"Organization {org_id} not authorized.")

    logger.debug("Organization %s authorized", payload["repository"]["owner"]["login"])
    return org_id

@app.route("/health", methods=['GET'])
def health():
    return "ok"

@app.route("/", methods=['POST'])
def webhook():
    import redis_client
    from worker import queue_lock, queue_event
    from runner import delete_pod, find_pod_by_job_id

    body = check_webhook_signature(request.headers, request.get_data(as_text=True))
    payload, action = check_webhook_event(body)

    if action == "queued":
        k8s_image, k8s_spec, job_labels = check_required_labels(payload)

        # This should be removed at some point as all organizations should be allowed
        authorize_organization(payload)

        job_id = payload["workflow_job"]["id"]
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
        job_id = payload["workflow_job"]["id"]
        if not job_id:
            raise WebhookError(400, "Missing workflow_job id in payload")

        r = redis_client.connect()
        with queue_lock:
            prev_status = redis_client.complete_job(r, job_id)

        if prev_status is None:
            # Job not in Redis — search k8s by job_id label as fallback
            pod = find_pod_by_job_id(job_id)
            if pod:
                logger.warning("Job %s not found in Redis, deleting pod %s found by label", job_id, pod.metadata.name)
                delete_pod(pod)
                return f"Job {job_id} not found in Redis, pod {pod.metadata.name} deleted."
            else:
                logger.warning("Job %s not found in Redis or k8s", job_id)
                return f"Job {job_id} not found."

        return f"Job {job_id} marked completed (was {prev_status})."

    else:
        raise WebhookError(200, f"Ignoring {action} job")
