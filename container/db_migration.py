"""Dual-write migration wrapper: writes to both PostgreSQL and Redis, reads from PostgreSQL.

Phase 2: PostgreSQL is source of truth. All reads come from PostgreSQL. All writes go
to PostgreSQL first (errors propagate), then to Redis (errors logged as warnings).
Redis is kept in sync for rollback safety.

The bootstrap_migration() function verifies that all Redis data exists in PostgreSQL
and logs any discrepancies (does not insert).
"""
from __future__ import annotations

import logging
from typing import Any, Iterator

import db as redis_db
import pg as pg_db
from pg import DuplicateRunnerNameException  # re-export for callers

logger = logging.getLogger(__name__)


# --- Bootstrap ---

def ensure_schema() -> None:
    """Create PostgreSQL schema, tables, and indexes. Idempotent."""
    pg_db.ensure_schema()


def bootstrap_migration() -> None:
    """Verify all Redis data exists in PostgreSQL. Logs discrepancies but does not insert."""
    ensure_schema()
    _verify_jobs()
    _verify_workers()
    logger.info("Bootstrap verification complete")


_ZOMBIE_JOB_ID = "65886322031"  # Ancient zombie job with incompatible schema (payload/k8s_spec fields)


def _verify_jobs() -> None:
    """Check that all Redis jobs exist in PostgreSQL. Log any missing."""
    redis_jobs = redis_db.get_all_jobs()
    missing = 0
    checked = 0
    for job in redis_jobs:
        job_id = job.get("job_id")
        if not job_id:
            continue
        if str(job_id) == _ZOMBIE_JOB_ID:
            continue

        checked += 1
        pg_job = pg_db.get_job(job_id)
        if not pg_job:
            logger.warning("VERIFY MISSING job_id=%s status=%s entity_id=%s — exists in Redis but not in PostgreSQL",
                           job_id, job.get("status"), job.get("entity_id") or job.get("org_id"))
            missing += 1

    logger.info("VERIFY JOBS checked=%d missing=%d total_redis=%d", checked, missing, len(redis_jobs))


def _verify_workers() -> None:
    """Check that all Redis workers exist in PostgreSQL. Log any missing."""
    redis_workers = set(redis_db.iter_workers())
    pg_workers = set(pg_db.iter_workers())
    only_redis = redis_workers - pg_workers
    if only_redis:
        for entity_id, k8s_pool, pod_name in only_redis:
            logger.warning("VERIFY MISSING worker pod_name=%s entity_id=%s k8s_pool=%s — exists in Redis but not in PostgreSQL",
                           pod_name, entity_id, k8s_pool)
    logger.info("VERIFY WORKERS redis=%d pg=%d missing_from_pg=%d", len(redis_workers), len(pg_workers), len(only_redis))


# --- Write operations (PG first, Redis second) ---

def store_job(job_id: int, entity_id: int, entity_name: str, entity_type: str | Any,
              repo_full_name: str, installation_id: int, labels: list[str],
              k8s_pool: str, k8s_image: str, html_url: str) -> bool:
    """Store a new job. Writes to PostgreSQL first (source of truth), then Redis."""
    # PostgreSQL first — errors propagate
    pg_result = pg_db.store_job(
        job_id, entity_id, entity_name, entity_type, repo_full_name,
        installation_id, labels, k8s_pool, k8s_image, html_url)

    # Redis second — errors logged as warnings
    try:
        redis_db.store_job(
            job_id, entity_id, entity_name, entity_type, repo_full_name,
            installation_id, labels, k8s_pool, k8s_image, html_url)
    except Exception as e:
        logger.warning("REDIS WARNING store_job(%s) redis failed: %s", job_id, e)

    return pg_result


def update_job_running(job_id: int) -> str | None:
    """Update job status to running. PostgreSQL first, then Redis."""
    pg_result = pg_db.update_job_running(job_id)

    try:
        redis_db.update_job_running(job_id)
    except Exception as e:
        logger.warning("REDIS WARNING update_job_running(%s) redis failed: %s", job_id, e)

    return pg_result


def update_job_completed(job_id: int) -> str | None:
    """Update job status to completed. PostgreSQL first, then Redis."""
    pg_result = pg_db.update_job_completed(job_id)

    try:
        redis_db.update_job_completed(job_id)
    except Exception as e:
        logger.warning("REDIS WARNING update_job_completed(%s) redis failed: %s", job_id, e)

    return pg_result


# --- Worker operations ---

def add_worker(entity_id: int, k8s_pool: str, pod_name: str,
               job_labels: list[str], k8s_image: str) -> None:
    """Add a worker. PostgreSQL first (collision detection), then Redis.

    Raises DuplicateRunnerNameException if pod_name already exists in PostgreSQL.
    """
    # PostgreSQL first — detects name collisions, errors propagate
    pg_db.add_worker(entity_id, k8s_pool, pod_name, job_labels, k8s_image)

    # Redis second
    try:
        redis_db.add_worker(entity_id, k8s_pool, pod_name)
    except Exception as e:
        logger.warning("REDIS WARNING add_worker(%s) redis failed: %s", pod_name, e)


def remove_worker(entity_id: int | str, k8s_pool: str, pod_name: str) -> None:
    """Mark worker as completed. PostgreSQL first, then Redis."""
    pg_db.remove_worker(entity_id, k8s_pool, pod_name)

    try:
        redis_db.remove_worker(entity_id, k8s_pool, pod_name)
    except Exception as e:
        logger.warning("REDIS WARNING remove_worker(%s) redis failed: %s", pod_name, e)


# --- Read operations (from PostgreSQL) ---

def get_pool_demand(entity_id: int | str, job_labels: list[str]) -> tuple[int, int]:
    """Return (job_count, worker_count) for an entity + label set. From PostgreSQL."""
    return pg_db.get_pool_demand(int(entity_id), job_labels)


def get_total_workers_for_entity(entity_id: int | str) -> int:
    """Return total worker count. From PostgreSQL."""
    return pg_db.get_total_workers_for_entity(int(entity_id))


def get_pending_jobs() -> list[str]:
    """Return pending job IDs in FIFO order. From PostgreSQL."""
    return pg_db.get_pending_jobs()


def iter_workers() -> Iterator[tuple[str, str, str]]:
    """Yield (entity_id, k8s_pool, pod_name) for active workers. From PostgreSQL."""
    return pg_db.iter_workers()


def get_job(job_id: int | str) -> dict[str, str]:
    """Return job dict. From PostgreSQL."""
    return pg_db.get_job(int(job_id))


def cleanup_job(job_id: int | str) -> None:
    """Remove a completed job. From both."""
    pg_db.cleanup_job(int(job_id))
    try:
        redis_db.cleanup_job(job_id)
    except Exception as e:
        logger.warning("REDIS WARNING cleanup_job(%s) redis failed: %s", job_id, e)


def get_all_active_job_ids() -> set[str]:
    """Return all active job IDs. From PostgreSQL."""
    return pg_db.get_all_active_job_ids()


def get_pool_usage() -> dict:
    """Return pool usage. From PostgreSQL."""
    return pg_db.get_pool_usage()


def get_all_jobs() -> list[dict[str, str]]:
    """Return all jobs. From PostgreSQL."""
    return pg_db.get_all_jobs()


def iter_completed_jobs():
    """Yield completed jobs. From PostgreSQL."""
    return pg_db.iter_completed_jobs()


def wait_for_job(timeout: int) -> None:
    """Block until a new job is published or timeout. Uses PostgreSQL LISTEN/NOTIFY."""
    return pg_db.wait_for_job(timeout)


# --- PostgreSQL-only operations (used by scheduler for worker status sync) ---

def sync_worker_status(pods: list[Any], failure_info_by_pod: dict[str, dict]) -> None:
    """Update worker status in PostgreSQL from k8s pod phases."""
    assert failure_info_by_pod is not None, "failure_info_by_pod must be a dict, not None"
    pg_db.sync_worker_status(pods, failure_info_by_pod)


def mark_orphaned_workers_completed(active_pod_names: set[str], known_worker_pod_names: list[str]) -> None:
    """Mark specific orphaned workers as completed in PostgreSQL."""
    pg_db.mark_orphaned_workers_completed(active_pod_names, known_worker_pod_names)
