import json
from unittest.mock import patch, MagicMock

from worker import (
    provision_pending_jobs,
    cleanup_completed_jobs,
    reconcile_orphan_pods,
)


def make_pod(name, phase="Running", job_id=None):
    """Helper to create a mock k8s pod object."""
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {"app": "rise-riscv-runner"}
    if job_id:
        pod.metadata.labels["riseproject.com/job_id"] = str(job_id)
    pod.metadata.annotations = {}
    pod.status.phase = phase
    return pod


def make_job(job_id, payload=None):
    """Helper to create a mock Redis job hash."""
    if payload is None:
        payload = {
            "organization": {"id": 1, "login": "test-org"},
            "installation": {"id": 100},
            "repository": {"id": 200, "full_name": "test-org/repo", "owner": {"login": "test-org"}},
            "workflow_job": {"id": job_id, "name": "test", "labels": ["rise", "ubuntu-24.04-riscv"]},
        }
    return {
        "status": "pending",
        "payload": json.dumps(payload),
        "k8s_image": "test-image:latest",
        "k8s_spec": json.dumps({"nodeSelector": {"riseproject.dev/board": "scw-em-rv1"}}),
        "job_labels": json.dumps(["rise", "ubuntu-24.04-riscv"]),
    }


# --- provision_pending_jobs tests ---

@patch("worker.redis_client")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.authenticate_app", return_value="token-123")
@patch("worker.ensure_runner_group_on_org", return_value=42)
@patch("worker.create_jit_runner_config_on_org", return_value="jit-config")
@patch("worker.provision_runner")
def test_provision_pending_jobs_success(
    mock_provision, mock_jit, mock_group, mock_auth, mock_slot, mock_rc
):
    """Test successful provisioning of a pending job."""
    r = MagicMock()
    job = make_job(111)

    mock_rc.get_pending_jobs.return_value = ["111"]
    mock_rc.get_job.return_value = job
    mock_rc.pick_job.return_value = True

    provision_pending_jobs(r)

    mock_rc.pick_job.assert_called_once_with(r, "111")
    mock_auth.assert_called_once()
    mock_group.assert_called_once()
    mock_jit.assert_called_once()
    mock_provision.assert_called_once()
    mock_rc.finish_provisioning.assert_called_once_with(r, 111, "rise-riscv-runner-111")


@patch("worker.redis_client")
@patch("worker.has_available_slot", return_value=False)
def test_provision_pending_jobs_no_capacity(mock_slot, mock_rc):
    """Test that jobs are skipped when no capacity is available."""
    r = MagicMock()
    mock_rc.get_pending_jobs.return_value = ["111"]
    mock_rc.get_job.return_value = make_job(111)

    provision_pending_jobs(r)

    mock_rc.pick_job.assert_not_called()
    mock_rc.finish_provisioning.assert_not_called()


@patch("worker.redis_client")
@patch("worker.has_available_slot", return_value=True)
def test_provision_pending_jobs_pick_fails(mock_slot, mock_rc):
    """Test that provisioning is skipped when pick_job returns False (job cancelled)."""
    r = MagicMock()
    mock_rc.get_pending_jobs.return_value = ["111"]
    mock_rc.get_job.return_value = make_job(111)
    mock_rc.pick_job.return_value = False

    provision_pending_jobs(r)

    mock_rc.finish_provisioning.assert_not_called()


@patch("worker.redis_client")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.authenticate_app", side_effect=Exception("Auth failed"))
def test_provision_pending_jobs_requeues_on_failure(mock_auth, mock_slot, mock_rc):
    """Test that a job is requeued when provisioning fails."""
    r = MagicMock()
    mock_rc.get_pending_jobs.return_value = ["111"]
    mock_rc.get_job.return_value = make_job(111)
    mock_rc.pick_job.return_value = True

    provision_pending_jobs(r)

    mock_rc.requeue_job.assert_called_once_with(r, 111)
    mock_rc.finish_provisioning.assert_not_called()


@patch("worker.redis_client")
def test_provision_pending_jobs_skips_missing_job(mock_rc):
    """Test that missing jobs in Redis are skipped."""
    r = MagicMock()
    mock_rc.get_pending_jobs.return_value = ["111"]
    mock_rc.get_job.return_value = None

    provision_pending_jobs(r)

    mock_rc.pick_job.assert_not_called()


# --- cleanup_completed_jobs tests ---

@patch("worker.redis_client")
@patch("worker.find_pod_by_job_id")
@patch("worker.delete_pod")
def test_cleanup_completed_jobs_deletes_pod(mock_delete, mock_find, mock_rc):
    """Test that completed jobs have their pods deleted and are cleaned up."""
    r = MagicMock()
    pod = make_pod("pod-111", phase="Succeeded", job_id="111")
    mock_rc.get_completed_jobs_with_pods.return_value = ["111"]
    mock_find.return_value = pod

    cleanup_completed_jobs(r)

    mock_delete.assert_called_once_with(pod)
    mock_rc.cleanup_job.assert_called_once_with(r, "111")


@patch("worker.redis_client")
@patch("worker.find_pod_by_job_id", return_value=None)
@patch("worker.delete_pod")
def test_cleanup_completed_jobs_no_pod(mock_delete, mock_find, mock_rc):
    """Test that cleanup proceeds even when no pod is found."""
    r = MagicMock()
    mock_rc.get_completed_jobs_with_pods.return_value = ["111"]

    cleanup_completed_jobs(r)

    mock_delete.assert_not_called()
    mock_rc.cleanup_job.assert_called_once_with(r, "111")


@patch("worker.redis_client")
@patch("worker.find_pod_by_job_id")
@patch("worker.delete_pod", side_effect=Exception("k8s error"))
def test_cleanup_completed_jobs_delete_fails(mock_delete, mock_find, mock_rc):
    """Test that cleanup is skipped when pod deletion fails."""
    r = MagicMock()
    pod = make_pod("pod-111", phase="Succeeded", job_id="111")
    mock_rc.get_completed_jobs_with_pods.return_value = ["111"]
    mock_find.return_value = pod

    cleanup_completed_jobs(r)

    mock_delete.assert_called_once_with(pod)
    mock_rc.cleanup_job.assert_not_called()


# --- reconcile_orphan_pods tests ---

@patch("worker.redis_client")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_reconcile_deletes_finished_orphan(mock_delete, mock_list, mock_rc):
    """Test that finished orphan pods (not in Redis) are deleted."""
    r = MagicMock()
    pod = make_pod("orphan-pod", phase="Succeeded", job_id="999")
    mock_list.return_value = [pod]
    mock_rc.get_active_jobs.return_value = set()

    reconcile_orphan_pods(r)

    mock_delete.assert_called_once_with(pod)


@patch("worker.redis_client")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_reconcile_skips_running_orphan(mock_delete, mock_list, mock_rc):
    """Test that running orphan pods are not deleted."""
    r = MagicMock()
    pod = make_pod("running-orphan", phase="Running", job_id="999")
    mock_list.return_value = [pod]
    mock_rc.get_active_jobs.return_value = set()

    reconcile_orphan_pods(r)

    mock_delete.assert_not_called()


@patch("worker.redis_client")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_reconcile_skips_tracked_running_pod(mock_delete, mock_list, mock_rc):
    """Test that tracked running pods are left alone."""
    r = MagicMock()
    pod = make_pod("tracked-pod", phase="Running", job_id="111")
    mock_list.return_value = [pod]
    mock_rc.get_active_jobs.return_value = {"111"}

    reconcile_orphan_pods(r)

    mock_delete.assert_not_called()
    mock_rc.complete_job.assert_not_called()


@patch("worker.redis_client")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_reconcile_completes_tracked_finished_pod(mock_delete, mock_list, mock_rc):
    """Test that tracked pods in Succeeded/Failed phase are marked completed and deleted."""
    r = MagicMock()
    pod = make_pod("finished-pod", phase="Succeeded", job_id="111")
    mock_list.return_value = [pod]
    mock_rc.get_active_jobs.return_value = {"111"}

    reconcile_orphan_pods(r)

    mock_rc.complete_job.assert_called_once_with(r, "111")
    mock_delete.assert_called_once_with(pod)


@patch("worker.redis_client")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_reconcile_skips_pod_without_job_id_label(mock_delete, mock_list, mock_rc):
    """Test that pods without riseproject.com/job_id label are skipped."""
    r = MagicMock()
    pod = make_pod("no-label-pod", phase="Succeeded")  # no job_id
    mock_list.return_value = [pod]
    mock_rc.get_active_jobs.return_value = set()

    reconcile_orphan_pods(r)

    mock_delete.assert_not_called()
    mock_rc.complete_job.assert_not_called()
