import datetime
import json
import logging
import random
import string
import threading
import time
import traceback

import db
from db import DuplicateRunnerNameException
import k8s
import github as gh
from constants import *

from flask import Flask, request, make_response

# Used for /health for now
app = Flask(__name__)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 15

def gh_reconcile():
    """
    Reconcile database state with GitHub API.

    For each active job, check GitHub for its actual status. If GitHub says
    completed but database disagrees, mark it completed. If GitHub says in_progress
    but database says pending, update to running.
    """
    jobs = db.get_all_jobs()
    if not jobs:
        return

    # Group jobs by installation_id to minimize auth calls
    jobs_by_installation = {}
    for job in jobs:
        if job.get("status") == "completed":
            continue
        installation_id = job.get("installation_id")
        if installation_id:
            jobs_by_installation.setdefault(installation_id, []).append(job)

    for installation_id, jobs in jobs_by_installation.items():
        try:
            entity_type = EntityType(jobs[0].get("entity_type", EntityType.ORGANIZATION))
            token = gh.authenticate_app(int(installation_id), entity_type=entity_type)
        except gh.GitHubAPIError as e:
            logger.error("Failed to authenticate for installation %s: %s", installation_id, e)
            continue

        for job in jobs:
            job_id = job["job_id"]
            repo = job.get("repo_full_name")
            if not repo:
                continue

            try:
                gh_status = gh.get_job_status(repo, job_id, token)
            except gh.GitHubAPIError as e:
                logger.error("Failed to get status for job %s: %s", job_id, e)
                continue

            db_status = job.get("status")
            if gh_status == "completed" and db_status != "completed":
                logger.info("GH reconcile: job %s is completed on GitHub (was %s in DB)", job_id, db_status)
                db.update_job_completed(job_id)
            elif gh_status == "in_progress" and db_status == "pending":
                logger.info("GH reconcile: job %s is in_progress on GitHub (was pending in DB)", job_id)
                db.update_job_running(job_id)


def demand_match():
    """
    Match demand (pending jobs) with supply (k8s workers).

    Iterates pending jobs in FIFO order. For each job, checks:
    1. Pool demand vs supply — skip if demand already met
    2. Org max_workers cap — skip if org is at capacity
    3. K8s node capacity — skip if no available slot
    Then provisions a runner.
    """
    pending_job_ids = db.get_pending_jobs()
    if not pending_job_ids:
        logger.debug("No pending jobs to process")
        return

    logger.info("Processing %d pending jobs: [%s]", len(pending_job_ids), ', '.join(pending_job_ids))

    # Cache per-org worker counts
    entity_worker_counts = {}

    for job_id in pending_job_ids:
        job = db.get_job(job_id)
        if not job:
            logger.warning("Job %s not found in DB, skipping", job_id)
            continue
        if job.get("status") != "pending":
            logger.info("Job %s status is %s, not pending, skipping", job_id, job.get("status"))
            continue

        k8s_pool = job.get("k8s_pool")
        k8s_image = job.get("k8s_image")
        installation_id = job.get("installation_id")
        entity_name = job.get("entity_name") or job.get("org_name")  # migration fallback
        labels = json.loads(job.get("job_labels", "[]"))
        entity_type = EntityType(job.get("entity_type", EntityType.ORGANIZATION))
        entity_id = job.get("entity_id") or job.get("org_id")  # migration fallback
        repo_full_name = job.get("repo_full_name")

        if not all([k8s_pool, k8s_image, installation_id, entity_name, entity_id, repo_full_name]):
            logger.warning("Job %s missing required fields, skipping", job_id)
            continue

        # Check demand vs supply (matched by entity_id + job_labels, not k8s_pool)
        job_count, worker_count = db.get_pool_demand(entity_id, labels)
        if job_count <= worker_count:
            logger.info("Job %s: entity %s labels %s demand met (jobs=%d, workers=%d)",
                        job_id, entity_id, labels, job_count, worker_count)
            continue

        # Check max_workers cap
        entity_config = ENTITY_CONFIG.get(int(entity_id), {"max_workers": 20})
        max_workers = entity_config.get("max_workers")
        if max_workers is not None:
            if entity_id not in entity_worker_counts:
                entity_worker_counts[entity_id] = db.get_total_workers_for_entity(entity_id)
            if entity_worker_counts[entity_id] >= max_workers:
                logger.info("Job %s: entity %s at max_workers (%d/%d)",
                            job_id, entity_name, entity_worker_counts[entity_id], max_workers)
                continue

        # Check k8s capacity
        node_selector = {"riseproject.dev/board": k8s_pool}
        if not k8s.has_available_slot(node_selector):
            logger.info("Job %s: no k8s capacity for pool %s", job_id, k8s_pool)
            continue

        # Reserve name in DB first — detects collision before creating k8s pod
        runner_name = None
        for _ in range(5):  # max retries for name collision
            suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
            candidate = f"rise-riscv-runner%s-{entity_id}-{suffix}" % ("" if PROD else "-staging")
            try:
                db.add_worker(entity_id, entity_name, k8s_pool, candidate, job_labels=labels, k8s_image=k8s_image)
                runner_name = candidate
                break
            except DuplicateRunnerNameException:
                logger.warning("Runner name %s collision, regenerating", candidate)
                continue

        if runner_name is None:
            logger.error("Failed to generate unique runner name for entity=%s pool=%s after retries", entity_name, k8s_pool)
            continue

        # Name reserved in DB, now safe to provision
        try:
            token = gh.authenticate_app(int(installation_id), entity_type=entity_type)

            if entity_type == EntityType.ORGANIZATION:
                group_id = gh.ensure_runner_group(entity_name, token, RUNNER_GROUP_NAME)
                jit_config = gh.create_jit_runner_config_org(token, group_id, labels, entity_name, runner_name)
            else:
                jit_config = gh.create_jit_runner_config_repo(token, labels, repo_full_name, runner_name)

            k8s.provision_runner(jit_config, runner_name, k8s_image, k8s_pool, entity_id)

            # Update local cache
            entity_worker_counts[entity_id] = entity_worker_counts.get(entity_id, 0) + 1

            logger.info("Provisioned runner %s for entity=%s pool=%s entity_type=%s", runner_name, entity_name, k8s_pool, entity_type.value)

        except Exception as e:
            logger.error("Failed to provision runner %s for entity=%s pool=%s, error: %s", runner_name, entity_name, k8s_pool, str(e))


def cleanup_pods():
    """
    Clean up completed/failed pods and stale job hashes.

    Lists all runner pods, deletes those in Succeeded/Failed phase, and
    removes them from their pool:workers set. Also syncs worker status
    in PostgreSQL from k8s pod phases.

    K8s pod phase -> worker status mapping:
      Pending   -> worker 'pending'   (pod scheduled, containers not yet started)
      Running   -> worker 'running'   (at least one container running)
      Succeeded -> worker 'completed' (all containers exited 0)
      Failed    -> worker 'completed' (at least one container failed)
      Unknown   -> no change          (pod state indeterminate, keep current)
    """
    # First get the list of workers from the database, then list pods from k8s. This is
    # to avoid the race condition where we delete a pod that was just provisioned
    # but not yet added to the database, which would cause it to be recreated immediately.
    # By getting the list of workers first, we ensure that we only delete pods
    # that are known to the database as active workers.
    workers = list(db.iter_workers())
    pods = k8s.list_pods()

    # Collect failure info for Failed pods before deletion
    failure_info_by_pod = {}
    for pod in pods:
        if pod.status.phase == "Failed":
            try:
                failure_info_by_pod[pod.metadata.name] = k8s.collect_pod_failure_info(pod)
            except Exception as e:
                logger.error("Failed to collect failure info for pod %s: %s", pod.metadata.name, e)

    # Sync worker status in PostgreSQL (pending->running, running/pending->completed)
    db.sync_worker_status(pods, failure_info_by_pod)

    for pod in pods:
        if pod.status.phase not in ("Succeeded", "Failed"):
            continue

        pod_name = pod.metadata.name
        pod_labels = pod.metadata.labels or {}
        entity_id = pod_labels.get("riseproject.com/entity_id") or pod_labels.get("riseproject.com/org_id")  # migration fallback
        k8s_pool = pod_labels.get("riseproject.com/board")

        try:
            k8s.delete_pod(pod)
        except Exception as e:
            logger.error("Failed to delete pod %s: %s", pod_name, e)
            continue

        if entity_id and k8s_pool:
            db.remove_worker(entity_id, k8s_pool, pod_name)

    # Detect orphaned workers (in DB but no corresponding k8s pod)
    active_pod_names = {p.metadata.name for p in pods}
    for entity_id, k8s_pool, pod_name in workers:
        if pod_name not in active_pod_names:
            logger.warning("Worker %s in entity_id %s pool %s has no corresponding pod, removing from DB", pod_name, entity_id, k8s_pool)
            db.remove_worker(entity_id, k8s_pool, pod_name)

    # Also mark orphaned workers in PostgreSQL (only workers we know about)
    known_worker_pod_names = [pod_name for _, _, pod_name in workers]
    db.mark_orphaned_workers_completed(active_pod_names, known_worker_pod_names)


# --- HTTP Handlers ---

@app.route("/health", methods=['GET'])
def health():
    return "ok"


_STATUS_COLORS = {"pending": "#d97706", "running": "#2563eb", "completed": "#16a34a"}

def _format_status(status):
    color = _STATUS_COLORS.get(status, "#666")
    return f'<span style="color:{color}">[{status:9s}]</span>'

def _format_labels(job_labels):
    """Format job_labels for display. Handles both list and JSON string."""
    if isinstance(job_labels, str):
        labels = json.loads(job_labels)
    else:
        labels = job_labels or []
    return ('[' + ", ".join(labels) + ']') if labels else "<none>"


def _format_timestamp(created_at):
    """Format a created_at value (datetime or unix float string) for display."""
    if not created_at:
        return "?"
    if isinstance(created_at, datetime.datetime):
        return created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    return datetime.datetime.fromtimestamp(float(created_at), tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def render_job(job):
    status = _format_status(job.get("status"))
    job_id = job.get("job_id", "?")
    repo = job.get("repo_full_name", "")
    html_url = job.get("html_url", "")
    labels = _format_labels(job.get("job_labels"))
    created_str = _format_timestamp(job.get("created_at"))
    link = f'<a href="{html_url}">{repo}#{job_id}</a>' if html_url else f"{repo}#{job_id}"
    return f'{status}  {created_str}  {labels}  {link}'


@app.route("/usage", methods=['GET'])
def usage():
    active_jobs, active_workers = db.get_active_jobs_and_workers()

    # Group by (entity_name, job_labels JSON string)
    groups = {}
    for job in active_jobs:
        labels_key = json.dumps(job["job_labels"])
        key = (job["entity_id"], labels_key)
        if key not in groups:
            groups[key] = {"entity_name": job["entity_name"], "k8s_pool": job["k8s_pool"], "jobs": [], "workers": []}
        groups[key]["jobs"].append(job)

    for worker in active_workers:
        labels_key = json.dumps(worker["job_labels"])
        key = (worker["entity_id"], labels_key)
        if key not in groups:
            groups[key] = {"entity_name": w["entity_name"], "k8s_pool": worker["k8s_pool"], "jobs": [], "workers": []}
        groups[key]["workers"].append(worker)

    lines = []
    for (_, labels_key), group in sorted(groups.items()):
        labels_display = _format_labels(labels_key)
        lines.append(f"=== {group['entity_name']} / {labels_display} ({group['k8s_pool']}) ===")
        if group["jobs"]:
            lines.append(f"  Jobs ({len(group['jobs'])}):")
            for job in sorted(group["jobs"], key=lambda j: j["created_at"]):
                lines.append(f'    - {render_job(job)}')
        else:
            lines.append("  Jobs: none")
        if group["workers"]:
            lines.append(f"  Workers ({len(group['workers'])}):")
            for w in sorted(group["workers"], key=lambda w: w["created_at"]):
                lines.append(f"    - {_format_status(w['status'])}  {_format_timestamp(w['created_at'])} {_format_labels(w["job_labels"])}  {w['pod_name']}")
                try:
                    events = k8s.get_pod_events(w["pod_name"])
                    if events:
                        for ev in events:
                            ts = ev.last_timestamp or ev.event_time or ev.metadata.creation_timestamp
                            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "unknown"
                            lines.append(f"        {ts_str}  [{ev.type}]  {ev.reason}: {ev.message}")
                    else:
                        lines.append("      Events: (none)")
                except Exception:
                    lines.append("      Events: (error fetching)")
        else:
            lines.append("  Workers: none")
        lines.append("")
    if not lines:
        lines.append("No active pools.")
    return make_response(f"<title>{'Usage - Prod' if PROD else 'Usage - Staging'}</title><pre>{chr(10).join(lines)}</pre>", 200, {"Content-Type": "text/html"})


@app.route("/history", methods=['GET'])
def history():
    jobs = db.get_all_jobs()

    # Sort by created_at descending (newest first)
    jobs.sort(key=lambda j: float(j.get("created_at", 0)), reverse=True)

    lines = []
    for job in jobs:
        lines.append(render_job(job))

    if not lines:
        lines.append("No jobs found.")

    return make_response(f"<title>{'History - Prod' if PROD else 'History - Staging'}</title><pre>{chr(10).join(lines)}</pre>", 200, {"Content-Type": "text/html"})


if __name__ == "__main__":
    # Set the logging level for all loggers to INFO
    logging.basicConfig(
        level=logging.INFO,
        format='%(pathname)s:%(lineno)d::%(funcName)s: [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Ensure PostgreSQL schema/tables exist
    db.ensure_schema()

    def http_worker():
        from waitress import serve

        HOST = "0.0.0.0"
        PORT = 8080

        print(f"Starting server on http://{HOST}:{PORT}")
        serve(app, host=HOST, port=PORT)

    http_thread = threading.Thread(target=http_worker, daemon=True)
    http_thread.start()

    while True:
        try:
            gh_reconcile()
            cleanup_pods()
            demand_match()
        except Exception as e:
            logger.error("Worker error: %s\n%s", e, traceback.format_exc())

        db.wait_for_job(POLL_INTERVAL)
