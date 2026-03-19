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

            redis_status = job.get("status")
            if gh_status == "completed" and redis_status != "completed":
                logger.info("GH reconcile: job %s is completed on GitHub (was %s in Redis)", job_id, redis_status)
                db.update_job_completed(job_id)
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
    entity_worker_counts = {}

    for job_id in pending_job_ids:
        job = db.get_job(job_id)
        if not job:
            logger.debug("Job %s not found in DB, skipping", job_id)
            continue
        if job.get("status") != "pending":
            logger.debug("Job %s status is %s, not pending, skipping", job_id, job.get("status"))
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

        # Check pool demand vs supply
        job_count, worker_count = db.get_pool_demand(entity_id, k8s_pool)
        if job_count <= worker_count:
            logger.debug("Job %s: pool %s:%s demand met (jobs=%d, workers=%d)",
                        job_id, entity_id, k8s_pool, job_count, worker_count)
            continue

        # Check max_workers cap
        entity_config = ENTITY_CONFIG.get(int(entity_id), {"max_workers": 20})
        max_workers = entity_config.get("max_workers")
        if max_workers is not None:
            if entity_id not in entity_worker_counts:
                entity_worker_counts[entity_id] = db.get_total_workers_for_entity(entity_id)
            if entity_worker_counts[entity_id] >= max_workers:
                logger.debug("Job %s: entity %s at max_workers (%d/%d)",
                            job_id, entity_name, entity_worker_counts[entity_id], max_workers)
                continue

        # Check k8s capacity
        node_selector = {"riseproject.dev/board": k8s_pool}
        if not k8s.has_available_slot(node_selector):
            logger.debug("Job %s: no k8s capacity for pool %s", job_id, k8s_pool)
            continue

        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
        runner_name = f"rise-riscv-runner%s-{entity_id}-{suffix}" % ("" if PROD else "-staging")

        # Provision
        try:
            token = gh.authenticate_app(int(installation_id), entity_type=entity_type)

            if entity_type == EntityType.ORGANIZATION:
                group_id = gh.ensure_runner_group(entity_name, token, RUNNER_GROUP_NAME)
                jit_config = gh.create_jit_runner_config_org(token, group_id, labels, entity_name, runner_name)
            else:
                jit_config = gh.create_jit_runner_config_repo(token, labels, repo_full_name, runner_name)

            k8s.provision_runner(jit_config, runner_name, k8s_image, k8s_pool, entity_id)

            db.add_worker(entity_id, k8s_pool, runner_name)

            # Update local cache
            entity_worker_counts[entity_id] = entity_worker_counts.get(entity_id, 0) + 1

            logger.info("Provisioned runner %s for entity=%s pool=%s entity_type=%s", runner_name, entity_name, k8s_pool, entity_type.value)

        except Exception as e:
            logger.error("Failed to provision runner %s for entity=%s pool=%s", runner_name, entity_name, k8s_pool)


def cleanup_pods():
    """
    Clean up completed/failed pods and stale job hashes.

    Lists all runner pods, deletes those in Succeeded/Failed phase, and
    removes them from their pool:workers set.
    """
    # First get the list of workers from redis, then list pods from k8s. This is
    # to avoid the race condition where we delete a pod that was just provisioned
    # but not yet added to Redis, which would cause it to be recreated immediately.
    # By getting the list of workers first, we ensure that we only delete pods
    # that are known to Redis as active workers.
    workers = list(db.iter_workers())
    pods = k8s.list_pods()

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

    for entity_id, k8s_pool, pod_name in workers:
        if not any(p.metadata.name == pod_name for p in pods):
            logger.warning("Worker %s in entity_id %s pool %s has no corresponding pod, removing from DB", pod_name, entity_id, k8s_pool)
            db.remove_worker(entity_id, k8s_pool, pod_name)

def cleanup_jobs():
    """Clean up old completed job hashes."""
    active_job_ids = db.get_all_active_job_ids()
    for job_id, data in db.iter_completed_jobs():
        if job_id and job_id not in active_job_ids:
            created_at = data.get("created_at")
            if not created_at:
                logger.debug("Checking completed job %s for cleanup: missing created_at field, cleaning up", job_id)
                db.cleanup_job(job_id)
            elif time.time() - float(created_at) > 15 * (24 * 60 * 60):  # 15 days
                logger.debug("Checking completed job %s for cleanup: job not active for more than 15 days, cleaning up", job_id)
                db.cleanup_job(job_id)
            else:
                logger.debug("Checking completed job %s for cleanup: job not active, but for less than 15 days", job_id)
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
