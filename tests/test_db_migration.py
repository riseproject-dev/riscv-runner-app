from unittest.mock import patch, MagicMock, call

import pytest

from constants import EntityType
from db_migration import (
    store_job,
    update_job_running,
    update_job_completed,
    add_worker,
    remove_worker,
    get_pool_demand,
    get_pending_jobs,
    DuplicateRunnerNameException,
)


# --- store_job ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_store_job_pg_first_then_redis(mock_redis, mock_pg):
    """Phase 2: PG result returned, both called."""
    mock_pg.store_job.return_value = True
    mock_redis.store_job.return_value = True

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is True  # PG result
    mock_pg.store_job.assert_called_once()
    mock_redis.store_job.assert_called_once()


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_store_job_pg_failure_propagates(mock_redis, mock_pg):
    """Phase 2: PG failure propagates (PG is source of truth)."""
    mock_pg.store_job.side_effect = Exception("PG down")

    with pytest.raises(Exception, match="PG down"):
        store_job(111, entity_id=1000, entity_name="test-org",
                  entity_type=EntityType.ORGANIZATION,
                  repo_full_name="test-org/repo", installation_id=999,
                  labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                  html_url="https://example.com")

    # Redis should NOT have been called (PG failed first)
    mock_redis.store_job.assert_not_called()


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_store_job_redis_failure_logged(mock_redis, mock_pg):
    """Phase 2: Redis failure is logged as warning, PG result returned."""
    mock_pg.store_job.return_value = True
    mock_redis.store_job.side_effect = Exception("Redis down")

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is True  # PG result returned despite Redis failure


# --- update_job_running ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_update_job_running_returns_pg(mock_redis, mock_pg):
    mock_pg.update_job_running.return_value = "pending"

    result = update_job_running(111)

    assert result == "pending"  # PG result
    mock_pg.update_job_running.assert_called_once_with(111)
    mock_redis.update_job_running.assert_called_once_with(111)


# --- update_job_completed ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_update_job_completed_returns_pg(mock_redis, mock_pg):
    mock_pg.update_job_completed.return_value = "running"

    result = update_job_completed(111)

    assert result == "running"  # PG result


# --- add_worker ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_add_worker_pg_first(mock_redis, mock_pg):
    """add_worker writes to PG first (for collision detection), then Redis."""
    add_worker(1000, "scw-em-rv1", "pod-1", job_labels=["rise"], k8s_image="img:latest")

    mock_pg.add_worker.assert_called_once_with(
        1000, "scw-em-rv1", "pod-1", ["rise"], "img:latest")
    mock_redis.add_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_add_worker_duplicate_raises(mock_redis, mock_pg):
    """DuplicateRunnerNameException from PG propagates to caller."""
    mock_pg.add_worker.side_effect = DuplicateRunnerNameException("collision")

    with pytest.raises(DuplicateRunnerNameException):
        add_worker(1000, "scw-em-rv1", "pod-1", job_labels=["rise"], k8s_image="img:latest")

    mock_redis.add_worker.assert_not_called()


# --- remove_worker ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_remove_worker_pg_first(mock_redis, mock_pg):
    """Phase 2: PG first, then Redis."""
    remove_worker(1000, "scw-em-rv1", "pod-1")

    mock_pg.remove_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")
    mock_redis.remove_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")


# --- get_pool_demand ---

@patch("db_migration.pg_db")
def test_get_pool_demand_from_pg(mock_pg):
    """Phase 2: reads from PG only, no Redis comparison."""
    mock_pg.get_pool_demand.return_value = (3, 1)

    result = get_pool_demand(1000, ["ubuntu-24.04-riscv"])

    assert result == (3, 1)
    mock_pg.get_pool_demand.assert_called_once_with(1000, ["ubuntu-24.04-riscv"])


# --- get_pending_jobs ---

@patch("db_migration.pg_db")
def test_get_pending_jobs_from_pg(mock_pg):
    """Phase 2: reads from PG only."""
    mock_pg.get_pending_jobs.return_value = ["333", "111"]

    result = get_pending_jobs()

    assert result == ["333", "111"]
