import functools
import json
import logging
import redis
import ssl
import threading
import time

from constants import PROD

logger = logging.getLogger(__name__)

queue_event = threading.Condition()

ENV_PREFIX = "prod" if PROD else "staging"


def _job_key(job_id):
    return f"{ENV_PREFIX}:job:{job_id}"
def _pool_jobs_key(org_id, k8s_pool):
    return f"{ENV_PREFIX}:pool:{org_id}:{k8s_pool}:jobs"
def _pool_workers_key(org_id, k8s_pool):
    return f"{ENV_PREFIX}:pool:{org_id}:{k8s_pool}:workers"


@functools.lru_cache(maxsize=1)
def _init_client():
    """Create a Redis connection from the REDIS_URL environment variable."""
    from constants import REDIS_URL
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, ssl_cert_reqs=ssl.CERT_NONE)


# --- Handler operations ---

def store_job(job_id, org_id, org_name, repo_full_name, installation_id, labels, k8s_pool, k8s_image, html_url):
    """Store a new job. Returns True if created, False if duplicate."""
    r = _init_client()
    key = _job_key(job_id)
    now = time.time()

    created = r.hsetnx(key, "status", "pending")
    if not created:
        logger.debug("Job %s already exists, skipping", job_id)
        return False

    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "status": "pending",
        "job_id": str(job_id),
        "org_id": str(org_id),
        "org_name": org_name,
        "repo_full_name": repo_full_name,
        "installation_id": str(installation_id),
        "job_labels": json.dumps(labels),
        "k8s_pool": k8s_pool,
        "k8s_image": k8s_image,
        "html_url": html_url,
        "created_at": str(now),
    })
    pipe.sadd(_pool_jobs_key(org_id, k8s_pool), str(job_id))
    pipe.execute()

    logger.info("Stored job %s for org %s pool %s", job_id, org_name, k8s_pool)
    with queue_event:
        queue_event.notify()
    return True


def update_job_running(job_id):
    """Update job status to running. Returns previous status or None."""
    r = _init_client()
    key = _job_key(job_id)
    data = r.hgetall(key)
    if not data:
        logger.debug("Job %s not found in Redis", job_id)
        return None

    prev_status = data.get("status")
    if prev_status == "running":
        logger.debug("Job %s is already running", job_id)
        return "running"

    r.hset(key, "status", "running")
    logger.info("Job %s status updated to running (was %s)", job_id, prev_status)
    return prev_status


def complete_job(job_id):
    """Complete a job: remove from pool sets. Returns previous status or None."""
    r = _init_client()
    key = _job_key(job_id)
    data = r.hgetall(key)
    if not data:
        logger.debug("Job %s not found in Redis", job_id)
        return None

    prev_status = data.get("status")
    org_id = data.get("org_id")
    k8s_pool = data.get("k8s_pool")

    pipe = r.pipeline()
    pipe.hset(key, "status", "completed")
    if org_id and k8s_pool:
        pipe.srem(_pool_jobs_key(org_id, k8s_pool), str(job_id))
    pipe.execute()

    logger.info("Job %s status updated to completed (was %s)", job_id, prev_status)
    return prev_status


# --- Worker operations ---


def get_pool_demand(org_id, k8s_pool):
    """Return (job_count, worker_count) for a pool."""
    r = _init_client()
    pipe = r.pipeline()
    pipe.scard(_pool_jobs_key(org_id, k8s_pool))
    pipe.scard(_pool_workers_key(org_id, k8s_pool))
    job_count, worker_count = pipe.execute()
    return job_count, worker_count


def get_total_workers_for_org(org_id):
    """Return total worker count across all pools for an org."""
    r = _init_client()
    total = 0
    for key in r.scan_iter(match=f"{ENV_PREFIX}:pool:{org_id}:*:workers"):
        total += r.scard(key)
    return total


def get_pending_jobs():
    """Return all pending job IDs in FIFO order (derived from job hashes)."""
    r = _init_client()
    pending = []
    for key in r.scan_iter(match=f"{ENV_PREFIX}:pool:*:jobs"):
        for job_id in r.smembers(key):
            data = r.hgetall(_job_key(job_id))
            if data.get("status") == "pending":
                pending.append((job_id, float(data.get("created_at", 0))))
    return [job_id for job_id, _ in sorted(pending, key=lambda x: x[1])]


def add_worker(org_id, k8s_pool, pod_name):
    """Add a worker pod to the pool."""
    r = _init_client()
    r.sadd(_pool_workers_key(org_id, k8s_pool), pod_name)
    logger.debug("Added worker %s to pool %s:%s", pod_name, org_id, k8s_pool)


def remove_worker(org_id, k8s_pool, pod_name):
    """Remove a worker pod from the pool."""
    r = _init_client()
    r.srem(_pool_workers_key(org_id, k8s_pool), pod_name)
    logger.debug("Removed worker %s from pool %s:%s", pod_name, org_id, k8s_pool)


def get_job(job_id):
    """Return the full job hash."""
    r = _init_client()
    return r.hgetall(_job_key(job_id))


def cleanup_job(job_id):
    """Remove a completed job hash."""
    r = _init_client()
    r.delete(_job_key(job_id))
    logger.debug("Cleaned up job %s", job_id)


def get_all_job_ids():
    """Return all job_ids across all pool:jobs sets."""
    r = _init_client()
    all_ids = set()
    for key in r.scan_iter(match=f"{ENV_PREFIX}:pool:*:jobs"):
        all_ids.update(r.smembers(key))
    return all_ids


def get_pool_usage():
    """Return detailed usage: {(org_id, pool): {org_name, jobs: [{job_id, status, repo}], workers: [name]}}."""
    r = _init_client()
    result = {}
    for key in r.scan_iter(match=f"{ENV_PREFIX}:pool:*:jobs"):
        parts = key.split(":")
        org_id, k8s_pool = parts[2], parts[3]
        job_ids = r.smembers(key)
        jobs = []
        org_name = org_id
        for jid in job_ids:
            data = r.hgetall(_job_key(jid))
            if data:
                if data.get("org_name"):
                    org_name = data["org_name"]
                jobs.append({
                    "job_id": jid,
                    "status": data.get("status", "unknown"),
                    "html_url": data.get("html_url", ""),
                })
        workers = list(r.smembers(_pool_workers_key(org_id, k8s_pool)))
        result[(org_id, k8s_pool)] = {"org_name": org_name, "jobs": jobs, "workers": workers}
    # Also pick up pools that only have workers but no jobs
    for key in r.scan_iter(match=f"{ENV_PREFIX}:pool:*:workers"):
        parts = key.split(":")
        org_id, k8s_pool = parts[2], parts[3]
        if (org_id, k8s_pool) not in result:
            workers = list(r.smembers(key))
            result[(org_id, k8s_pool)] = {"org_name": org_id, "jobs": [], "workers": workers}
    return result


def iter_completed_jobs():
    """Yield (job_id, data) for all completed job hashes not in any active pool."""
    r = _init_client()
    for key in r.scan_iter(match=f"{ENV_PREFIX}:job:*"):
        data = r.hgetall(key)
        if data.get("status") == "completed":
            job_id = data.get("job_id")
            # There are some old job hashes without job_id field
            if job_id is None:
                job_id = key.split(":")[-1]
            yield job_id, data
