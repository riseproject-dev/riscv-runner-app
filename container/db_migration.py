"""Dual-write migration wrapper: writes to both Redis and PostgreSQL, reads from Redis.

Phase 1: Redis is source of truth. Every read compares Redis vs PostgreSQL and logs
mismatches. Every write goes to both databases. PostgreSQL failures are logged but
don't affect application behavior.

The bootstrap_migration() function copies all historical Redis data into PostgreSQL
at startup (idempotent via ON CONFLICT DO NOTHING).
"""
from __future__ import annotations

import json
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
    """Migrate all Redis data to PostgreSQL. Idempotent (ON CONFLICT DO NOTHING)."""
    ensure_schema()
    _migrate_jobs()
    _migrate_workers()
    logger.info("Bootstrap migration complete")


_ZOMBIE_JOB_ID = "65886322031"  # Ancient zombie job with incompatible schema (payload/k8s_spec fields)


def _migrate_jobs() -> None:
    """Read all job hashes from Redis, upsert into PostgreSQL."""
    jobs = redis_db.get_all_jobs()
    migrated = 0
    skipped = 0
    for job in jobs:
        job_id = job.get("job_id")

        # Drop the 1 known zombie job with incompatible schema
        if str(job_id) == _ZOMBIE_JOB_ID:
            logger.warning("MIGRATION SKIP job_id=%s reason=zombie_incompatible_schema", job_id)
            skipped += 1
            continue

        # Handle legacy field mappings (staging jobs with org_id/org_name)
        entity_id = job.get("entity_id") or job.get("org_id")
        entity_name = job.get("entity_name") or job.get("org_name")
        entity_type = job.get("entity_type") or ("Organization" if job.get("org_id") else None)

        if not job_id or not entity_id:
            logger.warning("MIGRATION SKIP job_id=%s reason=missing_required_fields entity_id=%s", job_id, entity_id)
            skipped += 1
            continue

        try:
            raw_labels = json.loads(job.get("job_labels", "[]"))
        except (json.JSONDecodeError, TypeError):
            raw_labels = []

        try:
            pg_db.upsert_job(
                job_id=job_id,
                status=job.get("status", "completed"),
                entity_id=entity_id,
                entity_name=entity_name or "",
                entity_type=entity_type or "",
                repo_full_name=job.get("repo_full_name", ""),
                installation_id=job.get("installation_id") or 0,
                job_labels=raw_labels,
                k8s_pool=job.get("k8s_pool", ""),
                k8s_image=job.get("k8s_image", ""),
                html_url=job.get("html_url"),
                created_at=job.get("created_at", "0"),
            )
            migrated += 1
        except Exception as e:
            logger.error("MIGRATION ERROR job_id=%s error=%s", job_id, e)
            skipped += 1

    logger.info("MIGRATION JOBS migrated=%d skipped=%d total=%d", migrated, skipped, len(jobs))


def _migrate_workers() -> None:
    """Read all worker sets from Redis, upsert into PostgreSQL."""
    migrated = 0
    for entity_id, k8s_pool, pod_name in redis_db.iter_workers():
        try:
            pg_db.upsert_worker(pod_name, entity_id, k8s_pool)
            migrated += 1
            logger.info("MIGRATION WORKER pod_name=%s entity_id=%s k8s_pool=%s", pod_name, entity_id, k8s_pool)
        except Exception as e:
            logger.error("MIGRATION ERROR pod_name=%s error=%s", pod_name, e)
    logger.info("MIGRATION WORKERS migrated=%d", migrated)


# --- Wrapped job operations ---

def store_job(job_id: int, entity_id: int, entity_name: str, entity_type: str | Any,
              repo_full_name: str, installation_id: int, labels: list[str],
              k8s_pool: str, k8s_image: str, html_url: str) -> bool:
    """Store a new job. Writes to both Redis and PostgreSQL. Returns Redis result."""
    redis_result = redis_db.store_job(
        job_id, entity_id, entity_name, entity_type, repo_full_name,
        installation_id, labels, k8s_pool, k8s_image, html_url)

    try:
        pg_result = pg_db.store_job(
            job_id, entity_id, entity_name, entity_type, repo_full_name,
            installation_id, labels, k8s_pool, k8s_image, html_url)
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH store_job(%s): redis=%s pg=%s",
                         job_id, redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR store_job(%s) pg failed: %s", job_id, e)

    return redis_result


def update_job_running(job_id: int) -> str | None:
    """Update job status to running. Writes to both. Returns Redis result."""
    redis_result = redis_db.update_job_running(job_id)

    try:
        pg_result = pg_db.update_job_running(job_id)
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH update_job_running(%s): redis=%s pg=%s",
                         job_id, redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR update_job_running(%s) pg failed: %s", job_id, e)

    return redis_result


def update_job_completed(job_id: int) -> str | None:
    """Update job status to completed. Writes to both. Returns Redis result."""
    redis_result = redis_db.update_job_completed(job_id)

    try:
        pg_result = pg_db.update_job_completed(job_id)
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH update_job_completed(%s): redis=%s pg=%s",
                         job_id, redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR update_job_completed(%s) pg failed: %s", job_id, e)

    return redis_result


# --- Wrapped worker operations ---

def add_worker(entity_id: int, k8s_pool: str, pod_name: str,
               job_labels: list[str] | None = None, k8s_image: str | None = None) -> None:
    """Add a worker. Writes to PostgreSQL first (for collision detection), then Redis.

    Raises DuplicateRunnerNameException if pod_name already exists in PostgreSQL.
    """
    # PostgreSQL first — detects name collisions before any k8s pod is created
    pg_db.add_worker(entity_id, k8s_pool, pod_name, job_labels=job_labels, k8s_image=k8s_image)

    # Redis second (SADD is idempotent, won't detect collisions)
    redis_db.add_worker(entity_id, k8s_pool, pod_name)


def remove_worker(entity_id: int | str, k8s_pool: str, pod_name: str) -> None:
    """Mark worker as completed. Writes to both."""
    redis_db.remove_worker(entity_id, k8s_pool, pod_name)

    try:
        pg_db.remove_worker(entity_id, k8s_pool, pod_name)
    except Exception as e:
        logger.error("MIGRATION ERROR remove_worker(%s) pg failed: %s", pod_name, e)


# --- Wrapped read operations ---

def get_pool_demand(entity_id: int | str, k8s_pool: str) -> tuple[int, int]:
    """Return (job_count, worker_count). Reads from Redis, compares with PostgreSQL."""
    redis_result = redis_db.get_pool_demand(entity_id, k8s_pool)

    try:
        pg_result = pg_db.get_pool_demand(entity_id, k8s_pool)
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH get_pool_demand(%s, %s): redis=%s pg=%s",
                         entity_id, k8s_pool, redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR get_pool_demand(%s, %s) pg failed: %s",
                     entity_id, k8s_pool, e)

    return redis_result


def get_total_workers_for_entity(entity_id: int | str) -> int:
    """Return total worker count. Reads from Redis, compares with PostgreSQL."""
    redis_result = redis_db.get_total_workers_for_entity(entity_id)

    try:
        pg_result = pg_db.get_total_workers_for_entity(entity_id)
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH get_total_workers_for_entity(%s): redis=%s pg=%s",
                         entity_id, redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR get_total_workers_for_entity(%s) pg failed: %s",
                     entity_id, e)

    return redis_result


def get_pending_jobs() -> list[str]:
    """Return pending job IDs. Reads from Redis, compares with PostgreSQL."""
    redis_result = redis_db.get_pending_jobs()

    try:
        pg_result = pg_db.get_pending_jobs()
        if redis_result != pg_result:
            logger.error("MIGRATION MISMATCH get_pending_jobs: redis=%s pg=%s",
                         redis_result, pg_result)
    except Exception as e:
        logger.error("MIGRATION ERROR get_pending_jobs pg failed: %s", e)

    return redis_result


def iter_workers() -> Iterator[tuple[str, str, str]]:
    """Yield (entity_id, k8s_pool, pod_name) for active workers. From Redis."""
    # Compare as sets (order doesn't matter)
    redis_workers = list(redis_db.iter_workers())

    try:
        pg_workers = list(pg_db.iter_workers())
        redis_set = set(redis_workers)
        pg_set = set(pg_workers)
        if redis_set != pg_set:
            only_redis = redis_set - pg_set
            only_pg = pg_set - redis_set
            logger.error("MIGRATION MISMATCH iter_workers: only_in_redis=%s only_in_pg=%s",
                         only_redis, only_pg)
    except Exception as e:
        logger.error("MIGRATION ERROR iter_workers pg failed: %s", e)

    return iter(redis_workers)


def get_job(job_id: int | str) -> dict[str, str]:
    """Return job dict. From Redis."""
    return redis_db.get_job(job_id)


def cleanup_job(job_id: int | str) -> None:
    """Remove a completed job hash. From both."""
    redis_db.cleanup_job(job_id)
    try:
        pg_db.cleanup_job(job_id)
    except Exception as e:
        logger.error("MIGRATION ERROR cleanup_job(%s) pg failed: %s", job_id, e)


def get_all_active_job_ids() -> set[str]:
    """Return all active job IDs. From Redis."""
    return redis_db.get_all_active_job_ids()


def get_pool_usage() -> dict:
    """Return pool usage. From Redis."""
    return redis_db.get_pool_usage()


def get_all_jobs() -> list[dict[str, str]]:
    """Return all jobs. From Redis."""
    return redis_db.get_all_jobs()


def iter_completed_jobs():
    """Yield completed jobs. From Redis."""
    return redis_db.iter_completed_jobs()


def wait_for_job(timeout: int) -> None:
    """Block until a new job is published or timeout. Uses Redis pub/sub in Phase 1."""
    return redis_db.wait_for_job(timeout)


# --- PostgreSQL-only operations (used by scheduler for worker status sync) ---

def sync_worker_status(pods: list[Any], failure_info_by_pod: dict[str, dict]) -> None:
    """Update worker status in PostgreSQL from k8s pod phases."""
    assert failure_info_by_pod is not None, "failure_info_by_pod must be a dict, not None"
    try:
        pg_db.sync_worker_status(pods, failure_info_by_pod)
    except Exception as e:
        logger.error("MIGRATION ERROR sync_worker_status pg failed: %s", e)


def mark_orphaned_workers_completed(active_pod_names: set[str], known_worker_pod_names: list[str]) -> None:
    """Mark specific orphaned workers as completed in PostgreSQL."""
    try:
        pg_db.mark_orphaned_workers_completed(active_pod_names, known_worker_pod_names)
    except Exception as e:
        logger.error("MIGRATION ERROR mark_orphaned_workers_completed pg failed: %s", e)
