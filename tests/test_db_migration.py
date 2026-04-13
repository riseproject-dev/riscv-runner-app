from unittest.mock import patch, MagicMock, call

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
def test_store_job_dual_write(mock_redis, mock_pg):
    mock_redis.store_job.return_value = True
    mock_pg.store_job.return_value = True

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is True
    mock_redis.store_job.assert_called_once()
    mock_pg.store_job.assert_called_once()


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_store_job_pg_failure_returns_redis_result(mock_redis, mock_pg):
    mock_redis.store_job.return_value = True
    mock_pg.store_job.side_effect = Exception("PG down")

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is True  # Redis result returned despite PG failure


# --- update_job_running ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_update_job_running_dual_write(mock_redis, mock_pg):
    mock_redis.update_job_running.return_value = "pending"
    mock_pg.update_job_running.return_value = "pending"

    result = update_job_running(111)

    assert result == "pending"
    mock_redis.update_job_running.assert_called_once_with(111)
    mock_pg.update_job_running.assert_called_once_with(111)


# --- update_job_completed ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_update_job_completed_dual_write(mock_redis, mock_pg):
    mock_redis.update_job_completed.return_value = "running"
    mock_pg.update_job_completed.return_value = "running"

    result = update_job_completed(111)

    assert result == "running"


# --- add_worker ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_add_worker_pg_first(mock_redis, mock_pg):
    """add_worker writes to PG first (for collision detection), then Redis."""
    add_worker(1000, "scw-em-rv1", "pod-1", job_labels=["rise"], k8s_image="img:latest")

    # PG called with full args
    mock_pg.add_worker.assert_called_once_with(
        1000, "scw-em-rv1", "pod-1", job_labels=["rise"], k8s_image="img:latest")
    # Redis called without extra args
    mock_redis.add_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_add_worker_duplicate_raises(mock_redis, mock_pg):
    """DuplicateRunnerNameException from PG propagates to caller."""
    mock_pg.add_worker.side_effect = DuplicateRunnerNameException("collision")

    try:
        add_worker(1000, "scw-em-rv1", "pod-1")
        assert False, "Should have raised"
    except DuplicateRunnerNameException:
        pass

    # Redis should NOT have been called
    mock_redis.add_worker.assert_not_called()


# --- remove_worker ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_remove_worker_dual_write(mock_redis, mock_pg):
    remove_worker(1000, "scw-em-rv1", "pod-1")

    mock_redis.remove_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")
    mock_pg.remove_worker.assert_called_once_with(1000, "scw-em-rv1", "pod-1")


# --- get_pool_demand ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_get_pool_demand_returns_redis(mock_redis, mock_pg):
    mock_redis.get_pool_demand.return_value = (3, 1)
    mock_pg.get_pool_demand.return_value = (3, 1)

    result = get_pool_demand(1000, "scw-em-rv1")

    assert result == (3, 1)


@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_get_pool_demand_mismatch_logs_error(mock_redis, mock_pg):
    mock_redis.get_pool_demand.return_value = (3, 1)
    mock_pg.get_pool_demand.return_value = (2, 1)  # mismatch

    result = get_pool_demand(1000, "scw-em-rv1")

    assert result == (3, 1)  # Redis result returned


# --- get_pending_jobs ---

@patch("db_migration.pg_db")
@patch("db_migration.redis_db")
def test_get_pending_jobs_returns_redis(mock_redis, mock_pg):
    mock_redis.get_pending_jobs.return_value = ["333", "111"]
    mock_pg.get_pending_jobs.return_value = ["333", "111"]

    result = get_pending_jobs()

    assert result == ["333", "111"]
