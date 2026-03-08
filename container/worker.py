import json
import logging
import random
import string
import threading
import time
import traceback

import db
import k8s
import github as gh

from constants import *

logger = logging.getLogger(__name__)

POLL_INTERVAL = 15


def gh_reconcile():
    """
    Reconcile Redis state with GitHub API.

    For each active job, check GitHub for its actual status. If GitHub says
    completed but Redis disagrees, mark it completed. If GitHub says in_progress
    but Redis says pending, update to running.
    """
    active_job_ids = db.get_all_job_ids()
    if not active_job_ids:
        return

    # Group jobs by installation_id to minimize auth calls
    jobs_by_installation = {}
    for job_id in active_job_ids:
        job = db.get_job(job_id)
        if not job:
            continue
        if job.get("status") == "completed":
            continue
        inst_id = job.get("installation_id")
        if inst_id:
            jobs_by_installation.setdefault(inst_id, []).append(job)

    for installation_id, jobs in jobs_by_installation.items():
        try:
            token = gh.authenticate_app(int(installation_id))
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

            redis_status = job.get("status")
            if gh_status == "completed" and redis_status != "completed":
                logger.info("GH reconcile: job %s is completed on GitHub (was %s in Redis)", job_id, redis_status)
                db.complete_job(job_id)
            elif gh_status == "in_progress" and redis_status == "pending":
                logger.info("GH reconcile: job %s is in_progress on GitHub (was pending in Redis)", job_id)
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

    logger.debug("Processing %d pending jobs: [%s]", len(pending_job_ids), ', '.join(pending_job_ids))

    # Cache per-org worker counts
    org_worker_counts = {}

    for job_id in pending_job_ids:
        job = db.get_job(job_id)
        if not job:
            logger.debug("Job %s not found in DB, skipping", job_id)
            continue
        if job.get("status") != "pending":
            logger.debug("Job %s status is %s, not pending, skipping", job_id, job.get("status"))
            continue

        org_id = job.get("org_id")
        k8s_pool = job.get("k8s_pool")
        k8s_image = job.get("k8s_image")
        installation_id = job.get("installation_id")
        org_name = job.get("org_name")
        labels = json.loads(job.get("job_labels", "[]"))

        if not all([org_id, k8s_pool, k8s_image, installation_id, org_name]):
            logger.warning("Job %s missing required fields, skipping", job_id)
            continue

        # Check pool demand vs supply
        job_count, worker_count = db.get_pool_demand(org_id, k8s_pool)
        if job_count <= worker_count:
            logger.debug("Job %s: pool %s:%s demand met (jobs=%d, workers=%d)",
                        job_id, org_id, k8s_pool, job_count, worker_count)
            continue

        # Check org max_workers cap
        org_config = ORG_CONFIG.get(int(org_id), {"max_workers": 20}) # Default max_workers=20 for unknown orgs
        max_workers = org_config.get("max_workers")
        if max_workers is not None:
            if org_id not in org_worker_counts:
                org_worker_counts[org_id] = db.get_total_workers_for_org(org_id)
            if org_worker_counts[org_id] >= max_workers:
                logger.debug("Job %s: org %s at max_workers (%d/%d)",
                            job_id, org_name, org_worker_counts[org_id], max_workers)
                continue

        # Check k8s capacity
        node_selector = {"riseproject.dev/board": k8s_pool}
        if not k8s.has_available_slot(node_selector):
            logger.debug("Job %s: no k8s capacity for pool %s", job_id, k8s_pool)
            continue

        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
        runner_name = f"rise-riscv-runner%s-{org_id}-{suffix}" % ("" if PROD else "-staging")

        # Provision
        try:
            token = gh.authenticate_app(int(installation_id))
            group_id = gh.ensure_runner_group(org_name, token, RUNNER_GROUP_NAME)
            jit_config = gh.create_jit_runner_config(token, group_id, labels, org_name, runner_name)

            k8s.provision_runner(jit_config, runner_name, k8s_image, k8s_pool, org_id)

            db.add_worker(org_id, k8s_pool, runner_name)

            # Update local cache
            org_worker_counts[org_id] = org_worker_counts.get(org_id, 0) + 1

            logger.info("Provisioned runner %s for org=%s pool=%s", runner_name, org_name, k8s_pool)

        except Exception as e:
            logger.error("Failed to provision runner %s for org=%s pool=%s", runner_name, org_name, k8s_pool)


def cleanup_pods():
    """
    Clean up completed/failed pods and stale job hashes.

    Lists all runner pods, deletes those in Succeeded/Failed phase, and
    removes them from their pool:workers set.
    """
    pods = k8s.list_pods()
    for pod in pods:
        if pod.status.phase not in ("Succeeded", "Failed"):
            continue

        pod_name = pod.metadata.name
        pod_labels = pod.metadata.labels or {}
        org_id = pod_labels.get("riseproject.com/org_id")
        k8s_pool = pod_labels.get("riseproject.com/board")

        try:
            k8s.delete_pod(pod)
        except Exception as e:
            logger.error("Failed to delete pod %s: %s", pod_name, e)
            continue

        if org_id and k8s_pool:
            db.remove_worker(org_id, k8s_pool, pod_name)

def cleanup_jobs():
    """Clean up old completed job hashes."""
    active_job_ids = db.get_all_job_ids()
    for job_id, data in db.iter_completed_jobs():
        if job_id and job_id not in active_job_ids:
            created_at = float(data.get("created_at", 0))
            if time.time() - created_at > 300:  # 5 minutes
                logger.debug("Checking completed job %s for cleanup: job not active for more than 5 minutes", job_id)
                db.cleanup_job(job_id)
            else:
                logger.debug("Checking completed job %s for cleanup: job not active, but for less than 5 minutes", job_id)
        else:
            logger.debug("Checking completed job %s for cleanup: job still active, skipping", job_id)


def worker_loop():
    """Main worker loop."""
    while True:
        try:
            gh_reconcile()
            cleanup_pods()
            cleanup_jobs()
            demand_match()
        except Exception as e:
            logger.error("Worker error: %s\n%s", e, traceback.format_exc())

        with db.queue_event:
            db.queue_event.wait(timeout=POLL_INTERVAL)


def start_worker():
    """Start the background worker thread."""
    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()
    logger.info("Background worker started")
    return thread
