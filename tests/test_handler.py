import json
import pytest
from unittest.mock import patch, MagicMock

from handler import (
    WebhookError,
    check_webhook_signature,
    check_webhook_event,
    authorize_organization,
    compute_signature,
    match_labels_to_k8s,
    ALLOWED_ORGS,
    GHAPP_WEBHOOK_SECRET,
)


# --- Signature verification ---

def test_valid_signature():
    body = '{"action":"queued"}'
    expected_signature = compute_signature(body, GHAPP_WEBHOOK_SECRET).hexdigest()
    headers = {"X-Hub-Signature-256": f"sha256={expected_signature}"}

    result = check_webhook_signature(headers, body)
    assert result == body


def test_invalid_signature():
    headers = {"X-Hub-Signature-256": "sha256=invalid"}
    with pytest.raises(WebhookError) as exc:
        check_webhook_signature(headers, "")
    assert exc.value.status_code == 401


def test_missing_signature():
    with pytest.raises(WebhookError) as exc:
        check_webhook_signature({}, "")
    assert exc.value.status_code == 401


# --- Event parsing ---

def test_queued_event():
    body = json.dumps({"action": "queued", "workflow_job": {"id": 1, "name": "test", "labels": ["ubuntu-24.04-riscv"]}, "repository": {"full_name": "org/repo"}})
    payload, action = check_webhook_event(body)
    assert action == "queued"


def test_completed_event():
    body = json.dumps({"action": "completed", "workflow_job": {"id": 123, "name": "test", "labels": ["ubuntu-24.04-riscv"]}, "repository": {"full_name": "org/repo"}})
    payload, action = check_webhook_event(body)
    assert action == "completed"


def test_in_progress_event():
    body = json.dumps({"action": "in_progress", "workflow_job": {"id": 123, "name": "test", "labels": ["ubuntu-24.04-riscv"]}, "repository": {"full_name": "org/repo"}})
    payload, action = check_webhook_event(body)
    assert action == "in_progress"


def test_ignored_event():
    body = '{"action":"waiting"}'
    with pytest.raises(WebhookError) as exc:
        check_webhook_event(body)
    assert exc.value.status_code == 200
    assert "Ignoring action" in exc.value.message


def test_invalid_json():
    with pytest.raises(WebhookError) as exc:
        check_webhook_event("{")
    assert exc.value.status_code == 400


# --- Organization authorization ---

def test_authorized_user():
    org_id = list(ALLOWED_ORGS)[0]
    payload = {"repository": {"owner": {"id": org_id, "login": "riseproject-dev"}}}
    result = authorize_organization(payload)
    assert result == org_id


def test_unauthorized_user():
    payload = {"repository": {"owner": {"id": 1, "login": "unknown-org"}}}
    with pytest.raises(WebhookError) as exc:
        authorize_organization(payload)
    assert exc.value.status_code == 200
    assert "not authorized" in exc.value.message


# --- Label matching ---

def test_match_labels_riscv():
    k8s_pool, k8s_image = match_labels_to_k8s(["ubuntu-24.04-riscv"])
    assert k8s_pool == "scw-em-rv1"
    assert k8s_image == "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"


def test_match_labels_rvv():
    k8s_pool, k8s_image = match_labels_to_k8s(["ubuntu-24.04-riscv-rvv"])
    assert k8s_pool == "cloudv10x-rvv"
    assert k8s_image == "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"


def test_match_labels_unsupported():
    with pytest.raises(WebhookError) as exc:
        match_labels_to_k8s(["unsupported-label"])
    assert "missing required platform label" in exc.value.message


def test_match_labels_missing_platform():
    with pytest.raises(WebhookError) as exc:
        match_labels_to_k8s(["random-label"])
    assert "missing required platform label" in exc.value.message


# --- Webhook integration ---

@patch("db._init_client")
@patch("db.store_job", return_value=True)
def test_webhook_queued_stores_job(mock_store, mock_connect):
    """Test that a queued webhook stores the job in Redis."""
    from handler import app

    payload = {
        "action": "queued",
        "workflow_job": {"id": 12345, "name": "test", "labels": ["ubuntu-24.04-riscv"], "html_url": "https://github.com/riseproject-dev/sample/actions/runs/1/job/12345"},
        "repository": {"id": 100, "full_name": "riseproject-dev/sample", "owner": {"id": list(ALLOWED_ORGS)[0], "login": "riseproject-dev"}},
        "installation": {"id": 999},
    }
    body = json.dumps(payload)
    sig = "sha256=" + compute_signature(body, GHAPP_WEBHOOK_SECRET).hexdigest()

    mock_connect.return_value = MagicMock()

    with app.test_client() as client:
        resp = client.post("/", data=body, headers={
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        })
        assert resp.status_code == 200
        assert b"stored" in resp.data
        mock_store.assert_called_once_with(
            job_id=12345,
            org_id=list(ALLOWED_ORGS)[0],
            org_name="riseproject-dev",
            repo_full_name="riseproject-dev/sample",
            installation_id=999,
            labels=["ubuntu-24.04-riscv"],
            k8s_pool="scw-em-rv1",
            k8s_image="cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0",
            html_url="https://github.com/riseproject-dev/sample/actions/runs/1/job/12345",
        )


@patch("db._init_client")
@patch("db.update_job_running", return_value="pending")
def test_webhook_in_progress(mock_update, mock_connect):
    """Test that an in_progress webhook updates job status."""
    from handler import app

    payload = {
        "action": "in_progress",
        "workflow_job": {"id": 12345, "name": "test", "labels": ["ubuntu-24.04-riscv"]},
        "repository": {"full_name": "riseproject-dev/sample", "owner": {"id": list(ALLOWED_ORGS)[0], "login": "riseproject-dev"}},
    }
    body = json.dumps(payload)
    sig = "sha256=" + compute_signature(body, GHAPP_WEBHOOK_SECRET).hexdigest()

    mock_connect.return_value = MagicMock()

    with app.test_client() as client:
        resp = client.post("/", data=body, headers={
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        })
        assert resp.status_code == 200
        assert b"running" in resp.data
        mock_update.assert_called_once_with(12345)


@patch("db._init_client")
@patch("db.complete_job", return_value="running")
def test_webhook_completed(mock_complete, mock_connect):
    """Test that a completed webhook marks the job as completed."""
    from handler import app

    payload = {
        "action": "completed",
        "workflow_job": {"id": 12345, "name": "test", "labels": ["ubuntu-24.04-riscv"]},
        "repository": {"full_name": "riseproject-dev/sample", "owner": {"id": list(ALLOWED_ORGS)[0], "login": "riseproject-dev"}},
    }
    body = json.dumps(payload)
    sig = "sha256=" + compute_signature(body, GHAPP_WEBHOOK_SECRET).hexdigest()

    mock_connect.return_value = MagicMock()

    with app.test_client() as client:
        resp = client.post("/", data=body, headers={
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        })
        assert resp.status_code == 200
        assert b"completed" in resp.data
        mock_complete.assert_called_once_with(12345)
