from unittest.mock import MagicMock

from redis_client import get_completed_jobs_with_pods, ENV_PREFIX


def test_get_completed_jobs_returns_strings():
    """Completed job IDs must be plain strings, not tuples, so they can
    be safely interpolated into Kubernetes label selectors."""
    r = MagicMock()
    r.scan_iter.return_value = [
        f"{ENV_PREFIX}:job:111",
        f"{ENV_PREFIX}:job:222",
    ]
    r.hgetall.side_effect = lambda key: {
        f"{ENV_PREFIX}:job:111": {"status": "completed", "pod_name": "pod-111"},
        f"{ENV_PREFIX}:job:222": {"status": "completed", "pod_name": "pod-222"},
    }[key]

    result = get_completed_jobs_with_pods(r)

    assert result == ["111", "222"]
    for job_id in result:
        assert isinstance(job_id, str)


def test_get_completed_jobs_skips_non_completed():
    """Only jobs with status=completed and a pod_name should be returned."""
    r = MagicMock()
    r.scan_iter.return_value = [
        f"{ENV_PREFIX}:job:111",
        f"{ENV_PREFIX}:job:222",
        f"{ENV_PREFIX}:job:333",
    ]
    r.hgetall.side_effect = lambda key: {
        f"{ENV_PREFIX}:job:111": {"status": "completed", "pod_name": "pod-111"},
        f"{ENV_PREFIX}:job:222": {"status": "running", "pod_name": "pod-222"},
        f"{ENV_PREFIX}:job:333": {"status": "completed"},  # no pod_name
    }[key]

    result = get_completed_jobs_with_pods(r)

    assert result == ["111"]
