import datetime
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

    org_id = payload["repository"]["owner"]["id"]
    if org_id not in STAGING_ORGS:
        logger.debug("Received request for org %s, not in staging orgs, skipping proxy", org_id)
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


def authorize_organization(payload):
    """Authorize the organization."""
    org_id = payload["repository"]["owner"]["id"]
    if not org_id:
        raise WebhookError(400, "Organization ID is missing in payload")

    if org_id not in ALLOWED_ORGS:
        logger.warning("Organization %s (%s) not authorized",
                     payload["repository"]["owner"]["login"], org_id)
        raise WebhookError(200, f"Organization {org_id} not authorized.")

    logger.debug("Organization %s authorized", payload["repository"]["owner"]["login"])
    return org_id


def match_labels_to_k8s(org_id, job_labels):
    """
    Map workflow job labels to a k8s pool name and container image.

    Returns (k8s_pool, k8s_image) where k8s_pool is the board name string
    used as Redis pool key and pod label.
    """
    if job_labels == ["ubuntu-24.04-riscv"]:
        return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_24_04
    elif job_labels == ["ubuntu-24.04-riscv-rvv"]:
        return "cloudv10x-rvv", RUNNER_IMAGE_UBUNTU_24_04
    elif job_labels == ["ubuntu-26.04-riscv"]:
        return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_26_04
    elif job_labels == ["ubuntu-26.04-riscv-rvv"]:
        return "cloudv10x-rvv", RUNNER_IMAGE_UBUNTU_26_04
    # Special case(s) for PyTorch org
    elif org_id == PYTORCH_ORG_ID and job_labels == ["linux.riscv64"]:
        return "scw-em-rv1", RUNNER_IMAGE_UBUNTU_24_04
    else:
        raise WebhookError(200, f"Ignoring job: missing required platform label (got {job_labels})")


# --- Routes ---

@app.route("/health", methods=['GET'])
def health():
    return "ok"


@app.route("/usage", methods=['GET'])
def usage():
    pool_usage = db.get_pool_usage()
    lines = []
    for (_, k8s_pool), info in sorted(pool_usage.items()):
        lines.append(f"=== {info['org_name']} / {k8s_pool} ===")
        if info["jobs"]:
            lines.append(f"  Jobs ({len(info['jobs'])}):")
            status_sorted_key = {"pending": 0, "running": 1, "completed": 2}
            for job in sorted(info["jobs"], key=lambda j: (status_sorted_key.get(j["status"], 3), j["job_id"])):
                lines.append(f"    - {job['job_id']}  [{job['status']}]  <a href=\"{job['html_url']}\">{job['html_url']}</a>")
        else:
            lines.append("  Jobs: none")
        if info["workers"]:
            lines.append(f"  Workers ({len(info['workers'])}):")
            for w in sorted(info["workers"]):
                lines.append(f"    - {w}")
        else:
            lines.append("  Workers: none")
        lines.append("")
    if not lines:
        lines.append("No active pools.")
    return make_response("<pre>%s</pre>" % ("\n".join(lines)), 200, {"Content-Type": "text/html"})


@app.route("/history", methods=['GET'])
def history():
    jobs = db.get_all_jobs()

    # Sort by created_at descending (newest first)
    jobs.sort(key=lambda j: float(j.get("created_at", 0)), reverse=True)

    # Group by (org_name, k8s_pool)
    grouped = {}
    for job in jobs:
        org_name = job.get("org_name", job.get("org_id", "unknown"))
        k8s_pool = job.get("k8s_pool", "unknown")
        grouped.setdefault((org_name, k8s_pool), []).append(job)

    lines = []
    for (org_name, k8s_pool), pool_jobs in sorted(grouped.items()):
        lines.append(f"=== {org_name} / {k8s_pool} ({len(pool_jobs)} jobs) ===")
        status_style = {"pending": "#d97706", "running": "#2563eb", "completed": "#16a34a"}
        for job in pool_jobs:
            status = job.get("status", "unknown")
            job_id = job.get("job_id", "?")
            repo = job.get("repo_full_name", "")
            html_url = job.get("html_url", "")
            created_at = job.get("created_at", "")
            if created_at:
                ts = datetime.datetime.fromtimestamp(float(created_at), tz=datetime.timezone.utc)
                created_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                created_str = "?"
            color = status_style.get(status, "#666")
            link = f'<a href="{html_url}">{repo}</a>' if html_url else repo
            lines.append(f'  <span style="color:{color}">[{status:9s}]</span>  {created_str}  {link}  (job {job_id})')
        lines.append("")

    if not lines:
        lines.append("No jobs found.")

    return make_response("<pre>%s</pre>" % "\n".join(lines), 200, {"Content-Type": "text/html"})


@app.route("/", methods=['POST'])
def webhook():
    event, body = check_webhook_signature(request.headers, request.get_data(as_text=True))

    if event == "ping":
        return f"pong"

    elif event == "workflow_job":
        payload, action = check_webhook_event(body)

        authorize_organization(payload)

        job_id = payload["workflow_job"]["id"]
        if not job_id:
            raise WebhookError(400, "Job ID is missing in payload")

        # labels may be missing when no labels are defined
        job_labels = payload["workflow_job"]["labels"] or []

        org_id = payload["repository"]["owner"]["id"]
        if not org_id:
            raise WebhookError(400, "Organization ID is missing in payload")

        # Make sure the required labels are present; Filters out unsupported jobs early
        k8s_pool, k8s_image = match_labels_to_k8s(org_id, job_labels)

        logger.info("Received %s workflow_job id=%s name=%s repo=%s labels=%s",
                    action, job_id, payload["workflow_job"]["name"],
                    payload["repository"]["full_name"],
                    payload["workflow_job"]["labels"])

        if action == "queued":
            installation_id = payload["installation"]["id"]
            if not installation_id:
                raise WebhookError(400, "Installation ID is missing in payload")

            org_name = payload["repository"]["owner"]["login"]
            if not org_name:
                raise WebhookError(400, "Organization name is missing in payload")

            repo_full_name = payload["repository"]["full_name"]
            if not repo_full_name:
                raise WebhookError(400, "Repository full name is missing in payload")

            html_url = payload["workflow_job"]["html_url"]
            if not html_url:
                raise WebhookError(400, "HTML URL is missing in payload")

            stored = db.store_job(
                job_id=job_id,
                org_id=org_id,
                org_name=org_name,
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
                logger.warning("Job %s not found in Redis on in_progress event", job_id)
                return f"Job {job_id} not found."
            logger.info("Job %s marked running (was %s)", job_id, prev_status)
            return f"Job {job_id} marked running (was {prev_status})."

        elif action == "completed":
            prev_status = db.complete_job(job_id)
            if prev_status is None:
                logger.warning("Job %s not found in Redis on completed event", job_id)
                return f"Job {job_id} not found."
            return f"Job {job_id} completed (was {prev_status})."

        else:
            return f"Ignoring {action} job"

    else:
        return f"Ignoring {event} event"
