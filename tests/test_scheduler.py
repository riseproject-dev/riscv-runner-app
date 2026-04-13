import json
from unittest.mock import patch, MagicMock

from constants import EntityType
from scheduler import (
    demand_match,
    cleanup_pods,
    gh_reconcile,
)


def make_pod(name, phase="Running", entity_id=None, board=None):
    """Helper to create a mock k8s pod object."""
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {"app": "rise-riscv-runner"}
    if entity_id:
        pod.metadata.labels["riseproject.com/entity_id"] = str(entity_id)
    if board:
        pod.metadata.labels["riseproject.com/board"] = board
    pod.status.phase = phase
    return pod


def make_job(job_id, entity_id="1000", entity_name="test-org", k8s_pool="scw-em-rv1",
             status="pending", installation_id="999", repo_full_name="test-org/repo",
             entity_type=EntityType.ORGANIZATION):
    """Helper to create a mock job dict."""
    return {
        "status": status,
        "job_id": str(job_id),
        "entity_id": str(entity_id),
        "entity_name": entity_name,
        "entity_type": entity_type.value,
        "repo_full_name": repo_full_name,
        "installation_id": str(installation_id),
        "job_labels": json.dumps(["ubuntu-24.04-riscv"]),
        "k8s_pool": k8s_pool,
        "k8s_image": "test-image:latest",
        "created_at": "1000000.0",
    }


# --- demand_match tests ---

@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=True)
@patch("scheduler.k8s.provision_runner")
@patch("scheduler.gh.authenticate_app", return_value="token-123")
@patch("scheduler.gh.ensure_runner_group", return_value=42)
@patch("scheduler.gh.create_jit_runner_config_org", return_value="jit-config-encoded")
def test_demand_match_provisions_org_job(mock_jit, mock_group, mock_auth, mock_provision, mock_slot, mock_db):
    """Test that demand_match provisions a pending org job when capacity exists."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)  # 1 job, 0 workers = deficit
    mock_db.get_total_workers_for_entity.return_value = 0

    demand_match()

    mock_auth.assert_called_once_with(999, entity_type=EntityType.ORGANIZATION)
    mock_group.assert_called_once()
    mock_jit.assert_called_once()
    mock_provision.assert_called_once()
    mock_db.add_worker.assert_called_once()


@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=True)
@patch("scheduler.k8s.provision_runner")
@patch("scheduler.gh.authenticate_app", return_value="token-123")
@patch("scheduler.gh.create_jit_runner_config_repo", return_value="jit-config-repo")
def test_demand_match_provisions_personal_job(mock_jit_repo, mock_auth, mock_provision, mock_slot, mock_db):
    """Test that demand_match provisions a pending personal account job (repo-scoped)."""
    job = make_job(222, entity_id="200", entity_name="someuser", entity_type=EntityType.USER,
                   repo_full_name="someuser/myrepo")
    mock_db.get_pending_jobs.return_value = ["222"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_entity.return_value = 0

    demand_match()

    mock_auth.assert_called_once_with(999, entity_type=EntityType.USER)
    mock_jit_repo.assert_called_once()
    # Verify repo_full_name is passed
    call_args = mock_jit_repo.call_args
    assert call_args[0][2] == "someuser/myrepo"  # repo_full_name
    mock_provision.assert_called_once()
    mock_db.add_worker.assert_called_once()


@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=True)
@patch("scheduler.k8s.provision_runner")
def test_demand_match_skips_when_demand_met(mock_provision, mock_slot, mock_db):
    """Test that jobs are skipped when pool demand is already met."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 1)  # demand met

    demand_match()

    mock_provision.assert_not_called()


@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=False)
@patch("scheduler.k8s.provision_runner")
def test_demand_match_skips_no_k8s_capacity(mock_provision, mock_slot, mock_db):
    """Test that jobs are skipped when no k8s capacity."""
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_entity.return_value = 0

    demand_match()

    mock_provision.assert_not_called()


@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=True)
@patch("scheduler.k8s.provision_runner")
def test_demand_match_respects_max_workers(mock_provision, mock_slot, mock_db):
    """Test that max_workers cap is respected."""
    job = make_job(111, entity_id="660779", entity_name="luhenry")  # max_workers defaults to 20
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_entity.return_value = 20  # at default cap

    demand_match()

    mock_provision.assert_not_called()


@patch("scheduler.db")
@patch("scheduler.k8s.has_available_slot", return_value=True)
@patch("scheduler.k8s.provision_runner", side_effect=Exception("K8s error"))
@patch("scheduler.gh.authenticate_app", return_value="token-123")
@patch("scheduler.gh.ensure_runner_group", return_value=42)
@patch("scheduler.gh.create_jit_runner_config_org", return_value="jit-config")
def test_demand_match_handles_provision_failure(mock_jit, mock_group, mock_auth, mock_provision, mock_slot, mock_db):
    """Test that provisioning failure is handled gracefully.

    add_worker is called BEFORE provision_runner to reserve the pod name.
    If provisioning fails, the orphan worker (status=pending, no pod) will
    be cleaned up by cleanup_pods() orphan detection.
    """
    job = make_job(111)
    mock_db.get_pending_jobs.return_value = ["111"]
    mock_db.get_job.return_value = job
    mock_db.get_pool_demand.return_value = (1, 0)
    mock_db.get_total_workers_for_entity.return_value = 0

    demand_match()  # should not raise

    # add_worker is called before provision_runner to reserve the name
    mock_db.add_worker.assert_called_once()


# --- cleanup_pods tests ---

@patch("scheduler.db")
@patch("scheduler.k8s.list_pods")
@patch("scheduler.k8s.delete_pod")
def test_cleanup_deletes_succeeded_pod(mock_delete, mock_list, mock_db):
    """Test that succeeded pods are deleted and removed from worker pool."""
    pod = make_pod("pod-1", phase="Succeeded", entity_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_delete.assert_called_once_with(pod)
    mock_db.remove_worker.assert_called_once_with("1000", "scw-em-rv1", "pod-1")


@patch("scheduler.db")
@patch("scheduler.k8s.list_pods")
@patch("scheduler.k8s.delete_pod")
def test_cleanup_skips_running_pod(mock_delete, mock_list, mock_db):
    """Test that running pods are not deleted."""
    pod = make_pod("pod-1", phase="Running", entity_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_delete.assert_not_called()
    mock_db.remove_worker.assert_not_called()


@patch("scheduler.db")
@patch("scheduler.k8s.list_pods")
@patch("scheduler.k8s.delete_pod", side_effect=Exception("k8s error"))
def test_cleanup_handles_delete_failure(mock_delete, mock_list, mock_db):
    """Test that delete failure doesn't crash and doesn't remove worker."""
    pod = make_pod("pod-1", phase="Failed", entity_id="1000", board="scw-em-rv1")
    mock_list.return_value = [pod]
    mock_db.get_all_active_job_ids.return_value = set()
    mock_db.init_client.return_value = MagicMock(scan_iter=MagicMock(return_value=[]))

    cleanup_pods()

    mock_db.remove_worker.assert_not_called()


# --- gh_reconcile tests ---

@patch("scheduler.db")
@patch("scheduler.gh.authenticate_app", return_value="token-123")
@patch("scheduler.gh.get_job_status", return_value="completed")
def test_gh_reconcile_completes_job(mock_status, mock_auth, mock_db):
    """Test that reconciliation marks a job completed when GH says so."""
    job = make_job(111, status="running")
    mock_db.get_all_jobs.return_value = [job]

    gh_reconcile()

    mock_db.update_job_completed.assert_called_once_with("111")


@patch("scheduler.db")
@patch("scheduler.gh.authenticate_app", return_value="token-123")
@patch("scheduler.gh.get_job_status", return_value="in_progress")
def test_gh_reconcile_updates_running(mock_status, mock_auth, mock_db):
    """Test that reconciliation updates pending→running when GH says in_progress."""
    job = make_job(111, status="pending")
    mock_db.get_all_jobs.return_value = [job]

    gh_reconcile()

    mock_db.update_job_running.assert_called_once_with("111")


@patch("scheduler.db")
def test_gh_reconcile_no_active_jobs(mock_db):
    """Test that reconciliation is a no-op when no active jobs."""
    mock_db.get_all_jobs.return_value = []

    gh_reconcile()

    mock_db.update_job_completed.assert_not_called()
    mock_db.update_job_running.assert_not_called()
