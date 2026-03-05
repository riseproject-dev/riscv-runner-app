import functools
import json
import logging
import redis
import ssl
import time

from constants import *

logger = logging.getLogger(__name__)


ENV_PREFIX = "prod" if PROD else "staging"
PENDING_QUEUE = f"{ENV_PREFIX}:jobs:pending"
ACTIVE_JOBS = f"{ENV_PREFIX}:jobs:active"


def job_key(job_id):
    return f"{ENV_PREFIX}:job:{job_id}"

@functools.lru_cache(maxsize=1)
def connect():
    """Create a Redis connection from the REDIS_URL environment variable."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, ssl_cert_reqs=ssl.CERT_NONE)


def enqueue_job(r, job_id, payload, k8s_image, k8s_spec, job_labels):
    """Enqueue a new job. Returns True if enqueued, False if duplicate."""
    key = job_key(job_id)
    now = time.time()

    # HSETNX for idempotency — only set if job doesn't exist
    created = r.hsetnx(key, "status", "pending")
    if not created:
        logger.debug("Job %s already exists, skipping enqueue", job_id)
        return False

    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "status": "pending",
        "payload": json.dumps(payload),
        "org_id": payload["repository"]["owner"]["id"],
        "org_name": payload["repository"]["owner"]["login"],
        "installation_id": payload["installation"]["id"],
        "repo_id": payload["repository"]["id"],
        "job_id": job_id,
        "k8s_image": k8s_image,
        "k8s_spec": json.dumps(k8s_spec),
        "job_labels": json.dumps(job_labels),
        "created_at": str(now),
    })
    pipe.zadd(PENDING_QUEUE, {str(job_id): now})
    pipe.execute()

    logger.info("Enqueued job %s", job_id)
    return True


def complete_job(r, job_id):
    """Mark a job as completed. Returns the previous status and pod_name (if any)."""
    key = job_key(job_id)
    data = r.hgetall(key)

    if not data:
        logger.debug("Job %s not found in Redis", job_id)
        return None

    status = data.get("status")

    pipe = r.pipeline()
    if status == "pending":
        # Never provisioned — just remove from queue
        pipe.hset(key, "status", "completed")
        pipe.zrem(PENDING_QUEUE, str(job_id))
        pipe.execute()
        logger.info("Job %s completed before provisioning (was pending)", job_id)
    elif status in ("provisioning", "running"):
        # Mark completed — worker will clean up the pod
        pipe.hset(key, "status", "completed")
        pipe.execute()
        logger.info("Job %s marked completed (was %s)", job_id, status)
    else:
        logger.info("Job %s already in status %s, ignoring complete", job_id, status)

    return status


def pick_job(r, job_id):
    """Transition a pending job to provisioning. Returns False if job is no longer pending."""
    key = job_key(job_id)
    status = r.hget(key, "status")
    if status != "pending":
        return False
    r.hset(key, "status", "provisioning")
    return True


def finish_provisioning(r, job_id, pod_name):
    """Mark a job as running after successful pod creation."""
    key = job_key(job_id)
    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "status": "running",
        "pod_name": pod_name,
        "provisioned_at": str(time.time()),
    })
    pipe.zrem(PENDING_QUEUE, str(job_id))
    pipe.sadd(ACTIVE_JOBS, job_id)
    pipe.execute()


def requeue_job(r, job_id):
    """Return a failed provisioning job back to pending."""
    key = job_key(job_id)
    r.hset(key, "status", "pending")
    logger.info("Job %s requeued after provisioning failure", job_id)


def get_pending_jobs(r):
    """Return all pending job IDs in FIFO order."""
    return r.zrange(PENDING_QUEUE, 0, -1)


def get_job(r, job_id):
    """Return the full job hash."""
    return r.hgetall(job_key(job_id))


def get_completed_jobs_with_pods(r):
    """Return job IDs that are completed and have a pod_name set."""
    results = []
    for job_id in r.smembers(ACTIVE_JOBS):
        # Scan all jobs — in practice we'd maintain a reverse index,
        # but with small job counts this is fine
        pass

    # Alternative: scan all job keys
    for key in r.scan_iter(match=f"{ENV_PREFIX}:job:*"):
        data = r.hgetall(key)
        if data.get("status") == "completed" and data.get("pod_name"):
            job_id = key.split(":", 2)[2]
            results.append(job_id)

    return results


def cleanup_job(r, job_id):
    """Remove a completed job from tracking."""
    key = job_key(job_id)
    pipe = r.pipeline()
    pipe.srem(ACTIVE_JOBS, job_id)
    pipe.delete(key)
    pipe.execute()
    logger.debug("Cleaned up job %s", job_id)


def get_active_jobs(r):
    """Return the set of all active job IDs."""
    return r.smembers(ACTIVE_JOBS)
