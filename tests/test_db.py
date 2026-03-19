from unittest.mock import patch, MagicMock, call

from constants import EntityType
from db import (
    store_job,
    update_job_running,
    update_job_completed,
    get_pool_demand,
    get_pending_jobs,
    add_worker,
    remove_worker,
    ENV_PREFIX,
)


def make_mock_redis():
    r = MagicMock()
    pipe = MagicMock()
    r.pipeline.return_value = pipe
    return r, pipe


# --- store_job ---

@patch("db._init_client")
def test_store_job_new(mock_init):
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    r.hsetnx.return_value = True  # new job

    result = store_job(111, entity_id=1000, entity_name="test-org", entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://github.com/test-org/repo/actions/runs/1/job/111")

    assert result is True
    r.hsetnx.assert_called_once()
    pipe.hset.assert_called_once()
    pipe.sadd.assert_any_call(f"{ENV_PREFIX}:pool:1000:scw-em-rv1:jobs", "111")
    pipe.execute.assert_called_once()


@patch("db._init_client")
def test_store_job_duplicate(mock_init):
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    r.hsetnx.return_value = False  # duplicate

    result = store_job(111, entity_id=1000, entity_name="test-org", entity_type=EntityType.ORGANIZATION,
                       repo_full_name="test-org/repo", installation_id=999,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://github.com/test-org/repo/actions/runs/1/job/111")

    assert result is False
    pipe.execute.assert_not_called()


@patch("db._init_client")
def test_store_job_personal(mock_init):
    """Personal account job uses repo_id as entity_id."""
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    r.hsetnx.return_value = True

    result = store_job(222, entity_id=200, entity_name="someuser", entity_type=EntityType.USER,
                       repo_full_name="someuser/myrepo", installation_id=888,
                       labels=["rise"], k8s_pool="scw-em-rv1", k8s_image="img:latest",
                       html_url="https://github.com/someuser/myrepo/actions/runs/1/job/222")

    assert result is True
    pipe.sadd.assert_any_call(f"{ENV_PREFIX}:pool:200:scw-em-rv1:jobs", "222")


# --- update_job_running ---

@patch("db._init_client")
def test_update_job_running(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r
    r.hgetall.return_value = {"status": "pending", "org_id": "1000"}

    prev = update_job_running(111)

    assert prev == "pending"
    r.hset.assert_called_once()


@patch("db._init_client")
def test_update_job_running_not_found(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r
    r.hgetall.return_value = {}

    prev = update_job_running(111)

    assert prev is None


# --- update_job_completed ---

@patch("db._init_client")
def test_complete_job(mock_init):
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    r.hgetall.return_value = {"status": "running", "entity_id": "1000", "k8s_pool": "scw-em-rv1"}

    prev = update_job_completed(111)

    assert prev == "running"
    pipe.hset.assert_called_once()
    pipe.srem.assert_called_once_with(f"{ENV_PREFIX}:pool:1000:scw-em-rv1:jobs", "111")
    pipe.execute.assert_called_once()


@patch("db._init_client")
def test_complete_job_fallback_to_org_id(mock_init):
    """Jobs without entity_id field fall back to org_id (migration)."""
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    r.hgetall.return_value = {"status": "running", "org_id": "1000", "k8s_pool": "scw-em-rv1"}

    prev = update_job_completed(111)

    assert prev == "running"
    pipe.srem.assert_called_once_with(f"{ENV_PREFIX}:pool:1000:scw-em-rv1:jobs", "111")


@patch("db._init_client")
def test_complete_job_not_found(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r
    r.hgetall.return_value = {}

    prev = update_job_completed(111)

    assert prev is None


# --- get_pool_demand ---

@patch("db._init_client")
def test_get_pool_demand(mock_init):
    r, pipe = make_mock_redis()
    mock_init.return_value = r
    pipe.execute.return_value = [3, 1]  # 3 jobs, 1 worker

    jobs, workers = get_pool_demand(1000, "scw-em-rv1")

    assert jobs == 3
    assert workers == 1


# --- get_pending_jobs ---

@patch("db._init_client")
def test_get_pending_jobs(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r
    r.scan_iter.return_value = [f"{ENV_PREFIX}:pool:1000:scw-em-rv1:jobs"]
    r.smembers.return_value = {"111", "222", "333"}
    r.hgetall.side_effect = lambda key: {
        f"{ENV_PREFIX}:job:111": {"status": "pending", "created_at": "1000002.0"},
        f"{ENV_PREFIX}:job:222": {"status": "running", "created_at": "1000001.0"},
        f"{ENV_PREFIX}:job:333": {"status": "pending", "created_at": "1000000.0"},
    }.get(key, {})

    result = get_pending_jobs()

    assert result == ["333", "111"]  # sorted by created_at, running job excluded


@patch("db._init_client")
def test_get_pending_jobs_empty(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r
    r.scan_iter.return_value = []

    result = get_pending_jobs()

    assert result == []


# --- add/remove worker ---

@patch("db._init_client")
def test_add_worker(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r

    add_worker(1000, "scw-em-rv1", "pod-1")

    r.sadd.assert_called_once_with(f"{ENV_PREFIX}:pool:1000:scw-em-rv1:workers", "pod-1")


@patch("db._init_client")
def test_remove_worker(mock_init):
    r, _ = make_mock_redis()
    mock_init.return_value = r

    remove_worker(1000, "scw-em-rv1", "pod-1")

    r.srem.assert_called_once_with(f"{ENV_PREFIX}:pool:1000:scw-em-rv1:workers", "pod-1")
