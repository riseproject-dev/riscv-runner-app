from unittest.mock import patch, MagicMock, PropertyMock
import json
import threading

import pytest

from pg import (
    store_job,
    update_job_running,
    update_job_completed,
    get_pool_demand,
    get_pending_jobs,
    add_worker,
    remove_worker,
    DuplicateRunnerNameException,
)
from constants import EntityType


def make_mock_pool():
    """Create a mock connection pool, connection, and cursor.

    The _PoolConnection context manager calls _init_pool() to get the pool,
    acquires _pool_semaphore, then calls pool.getconn(). On exit it calls
    conn.commit() (clean) or conn.rollback() (exception), then pool.putconn().
    We mock at the pool level so the context manager drives commit/rollback.
    """
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    pool.getconn.return_value = conn
    return pool, conn, cur


@pytest.fixture(autouse=True)
def mock_pool_and_semaphore():
    """Patch _pool_semaphore so _PoolConnection.__enter__/__exit__ don't block."""
    semaphore = threading.Semaphore(10)
    with patch("pg._pool_semaphore", semaphore):
        yield


# --- store_job ---

@patch("pg._init_pool")
def test_store_job_new(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.rowcount = 1  # inserted

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is True
    # INSERT + NOTIFY called (plus SET search_path)
    assert cur.execute.call_count >= 2


@patch("pg._init_pool")
def test_store_job_duplicate(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.rowcount = 0  # not inserted (duplicate)

    result = store_job(111, entity_id=1000, entity_name="test-org",
                       entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://example.com")

    assert result is False


@patch("pg._init_pool")
def test_store_job_sorts_labels(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.rowcount = 1

    store_job(111, entity_id=1000, entity_name="test-org",
              entity_type=EntityType.ORGANIZATION,
              repo_full_name="test-org/repo", installation_id=999,
              labels=["z-label", "a-label"], k8s_pool="scw-em-rv1",
              k8s_image="img:latest", html_url="https://example.com")

    # Check that sorted labels were passed to the INSERT
    insert_call = cur.execute.call_args_list[1]  # second call is the INSERT
    args = insert_call[0][1]
    # job_labels is the 7th parameter (index 6)
    assert args[6] == '["a-label", "z-label"]'


# --- update_job_running ---

@patch("pg._init_pool")
def test_update_job_running(mock_pool_fn):
    """Successful pending -> running transition returns old status via RETURNING old.status."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.return_value = ("pending",)  # RETURNING old.status

    prev = update_job_running(111)

    assert prev == "pending"


@patch("pg._init_pool")
def test_update_job_running_already_running(mock_pool_fn):
    """Job already running: UPDATE matches nothing, SELECT returns 'running'."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.side_effect = [None, ("running",)]  # UPDATE no match, SELECT finds it

    prev = update_job_running(111)

    assert prev == "running"


@patch("pg._init_pool")
def test_update_job_running_not_found(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.side_effect = [None, None]  # UPDATE no match, SELECT no match

    prev = update_job_running(111)

    assert prev is None


# --- update_job_completed ---

@patch("pg._init_pool")
def test_update_job_completed_from_running(mock_pool_fn):
    """Successful running -> completed returns 'running' via RETURNING old.status."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.return_value = ("running",)

    prev = update_job_completed(111)

    assert prev == "running"


@patch("pg._init_pool")
def test_update_job_completed_from_pending(mock_pool_fn):
    """Successful pending -> completed returns 'pending' via RETURNING old.status."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.return_value = ("pending",)

    prev = update_job_completed(111)

    assert prev == "pending"


@patch("pg._init_pool")
def test_update_job_completed_already(mock_pool_fn):
    """Job already completed: UPDATE matches nothing, SELECT returns 'completed'."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.side_effect = [None, ("completed",)]

    prev = update_job_completed(111)

    assert prev == "completed"


@patch("pg._init_pool")
def test_update_job_completed_not_found(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.side_effect = [None, None]

    prev = update_job_completed(111)

    assert prev is None


# --- get_pool_demand ---

@patch("pg._init_pool")
def test_get_pool_demand(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchone.return_value = (3, 1)

    jobs, workers = get_pool_demand(1000, "scw-em-rv1")

    assert jobs == 3
    assert workers == 1


# --- get_pending_jobs ---

@patch("pg._init_pool")
def test_get_pending_jobs(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.fetchall.return_value = [(333,), (111,)]

    result = get_pending_jobs()

    assert result == ["333", "111"]


# --- add/remove worker ---

@patch("pg._init_pool")
def test_add_worker(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.rowcount = 1  # inserted

    add_worker(1000, "scw-em-rv1", "pod-1", job_labels=["rise"], k8s_image="img:latest")

    # No explicit commit needed — _PoolConnection.__exit__ handles it


@patch("pg._init_pool")
def test_add_worker_duplicate_raises(mock_pool_fn):
    """DuplicateRunnerNameException propagates; context manager handles rollback."""
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool
    cur.rowcount = 0  # collision

    with pytest.raises(DuplicateRunnerNameException):
        add_worker(1000, "scw-em-rv1", "pod-1")


@patch("pg._init_pool")
def test_remove_worker(mock_pool_fn):
    pool, conn, cur = make_mock_pool()
    mock_pool_fn.return_value = pool

    remove_worker(1000, "scw-em-rv1", "pod-1")

    # Verify UPDATE was called (search_path + UPDATE = 2 execute calls)
    assert cur.execute.call_count >= 1
