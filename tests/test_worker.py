import json
from unittest.mock import patch, MagicMock

from worker import (
    demand_match,
    cleanup_pods,
    gh_reconcile,
)


def make_pod(name, phase="Running", org_id=None, board=None):
    """Helper to create a mock k8s pod object."""
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {"app": "rise-riscv-runner"}
    if org_id:
        pod.metadata.labels["riseproject.com/org_id"] = str(org_id)
    if board:
        pod.metadata.labels["riseproject.com/board"] = board
    pod.status.phase = phase
    return pod


def make_job(job_id, org_id="1000", org_name="test-org", k8s_pool="scw-em-rv1",
             status="pending", installation_id="999", repo_full_name="test-org/repo"):
    """Helper to create a mock Redis job hash."""
    return {
        "status": status,
        "job_id": str(job_id),
        "org_id": str(org_id),
        "org_name": org_name,
        "repo_full_name": repo_full_name,
        "installation_id": str(installation_id),
        "job_labels": json.dumps(["ubuntu-24.04-riscv"]),
        "k8s_pool": k8s_pool,
        "k8s_image": "test-image:latest",
        "created_at": "1000000.0",
    }


# --- demand_match tests ---

@patch("worker.db")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.provision_runner")
@patch("worker.authenticate_app", return_value="token-123")
@patch("worker.ensure_runner_group", return_value=42)
@patch("worker.create_jit_runner_config", return_value="jit-config-encoded")
def test_demand_match_provisions_job(mock_jit, mock_group, mock_auth, mock_provision, mock_slot, mock_db):
    """Test that demand_match provisions a pending job when capacity exists."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)  # 1 job, 0 workers = deficit
    mock_db.get_total_workers_for_org.return_value = 0

    demand_match()

    mock_provision.assert_called_once()
    mock_db.mark_provisioned.assert_called_once()
    mock_db.add_worker.assert_called_once()


@patch("worker.db")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.provision_runner")
def test_demand_match_skips_when_demand_met(mock_provision, mock_slot, mock_db):
    """Test that jobs are skipped when pool demand is already met."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 1)  # demand met

    demand_match()

    mock_provision.assert_not_called()


@patch("worker.db")
@patch("worker.has_available_slot", return_value=False)
@patch("worker.provision_runner")
def test_demand_match_skips_no_k8s_capacity(mock_provision, mock_slot, mock_db):
    """Test that jobs are skipped when no k8s capacity."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_org.return_value = 0

    demand_match()

    mock_provision.assert_not_called()


@patch("worker.db")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.provision_runner")
def test_demand_match_respects_max_workers(mock_provision, mock_slot, mock_db):
    """Test that max_workers cap is respected."""
    job = make_job(111, org_id="660779", org_name="luhenry")  # max_workers=5
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_org.return_value = 5  # at cap

    demand_match()

    mock_provision.assert_not_called()


@patch("worker.db")
@patch("worker.has_available_slot", return_value=True)
@patch("worker.provision_runner", side_effect=Exception("K8s error"))
@patch("worker.authenticate_app", return_value="token-123")
@patch("worker.ensure_runner_group", return_value=42)
@patch("worker.create_jit_runner_config", return_value="jit-config")
def test_demand_match_handles_provision_failure(mock_jit, mock_group, mock_auth, mock_provision, mock_slot, mock_db):
    """Test that provisioning failure is handled gracefully."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_org.return_value = 0

    demand_match()  # should not raise

    mock_db.mark_provisioned.assert_not_called()
    mock_db.add_worker.assert_not_called()


# --- cleanup_pods tests ---

@patch("worker.db")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_cleanup_deletes_succeeded_pod(mock_delete, mock_list, mock_db):
    """Test that succeeded pods are deleted and removed from worker pool."""
    pod = make_pod("pod-1", phase="Succeeded", org_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_delete.assert_called_once_with(pod)
    mock_db.remove_worker.assert_called_once_with("1000", "scw-em-rv1", "pod-1")


@patch("worker.db")
@patch("worker.list_pods")
@patch("worker.delete_pod")
def test_cleanup_skips_running_pod(mock_delete, mock_list, mock_db):
    """Test that running pods are not deleted."""
    pod = make_pod("pod-1", phase="Running", org_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_delete.assert_not_called()
    mock_db.remove_worker.assert_not_called()


@patch("worker.db")
@patch("worker.list_pods")
@patch("worker.delete_pod", side_effect=Exception("k8s error"))
def test_cleanup_handles_delete_failure(mock_delete, mock_list, mock_db):
    """Test that delete failure doesn't crash and doesn't remove worker."""
    pod = make_pod("pod-1", phase="Failed", org_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_db.remove_worker.assert_not_called()


# --- gh_reconcile tests ---

@patch("worker.db")
@patch("worker.authenticate_app", return_value="token-123")
@patch("worker.get_job_status", return_value="completed")
def test_gh_reconcile_completes_job(mock_status, mock_auth, mock_db):
    """Test that reconciliation marks a job completed when GH says so."""
    job = make_job(111, status="running")
    mock_db.get_all_active_job_ids.return_value = {"111"}
    mock_db.get_job.return_value = job

    gh_reconcile()

    mock_db.complete_job.assert_called_once_with("111")


@patch("worker.db")
@patch("worker.authenticate_app", return_value="token-123")
@patch("worker.get_job_status", return_value="in_progress")
def test_gh_reconcile_updates_running(mock_status, mock_auth, mock_db):
    """Test that reconciliation updates pending→running when GH says in_progress."""
    job = make_job(111, status="pending")
    mock_db.get_all_active_job_ids.return_value = {"111"}
    mock_db.get_job.return_value = job

    gh_reconcile()

    mock_db.update_job_running.assert_called_once_with("111")


@patch("worker.db")
def test_gh_reconcile_no_active_jobs(mock_db):
    """Test that reconciliation is a no-op when no active jobs."""
    mock_db.get_all_active_job_ids.return_value = set()

    gh_reconcile()

    mock_db.complete_job.assert_not_called()
    mock_db.update_job_running.assert_not_called()
