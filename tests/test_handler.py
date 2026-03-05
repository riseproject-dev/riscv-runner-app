import json
import pytest
from unittest.mock import patch, MagicMock

from handler import (
    ALLOWED_ORGS,
    WebhookError,
    check_webhook_signature,
    check_webhook_event,
    check_required_labels,
    authorize_organization,
    compute_signature,
    GHAPP_WEBHOOK_SECRET,
)

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


def test_queued_event():
    body = json.dumps({"action": "queued", "workflow_job": {"id": 1, "name": "test"}, "repository": {"full_name": "org/repo"}})
    payload, action = check_webhook_event(body)
    assert action == "queued"
    assert payload["action"] == "queued"


def test_completed_event():
    body = json.dumps({"action": "completed", "workflow_job": {"id": 123}, "repository": {"full_name": "org/repo"}})
    payload, action = check_webhook_event(body)
    assert action == "completed"
    assert payload["action"] == "completed"


def test_ignored_event():
    body = '{"action":"in_progress"}'
    with pytest.raises(WebhookError) as exc:
        check_webhook_event(body)
    assert exc.value.status_code == 200
    assert "Ignoring action" in exc.value.message


def test_invalid_json():
    with pytest.raises(WebhookError) as exc:
        check_webhook_event("{")
    assert exc.value.status_code == 400


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


def test_check_required_labels_valid():
    payload = {"workflow_job": {"labels": ["rise", "ubuntu-24.04-riscv"]}}
    k8s_image, k8s_spec, job_labels = check_required_labels(payload)
    assert k8s_image == "cloudv10x/github-actions-riscv:docker-ubuntu-2.331.0"
    assert "nodeSelector" in k8s_spec
    assert sorted(job_labels) == ["rise", "ubuntu-24.04-riscv"]


def test_check_required_labels_missing_rise():
    payload = {"workflow_job": {"labels": ["ubuntu-24.04-riscv"]}}
    with pytest.raises(WebhookError) as exc:
        check_required_labels(payload)
    assert exc.value.status_code == 200
    assert "missing required 'rise' label" in exc.value.message


def test_check_required_labels_unsupported():
    payload = {"workflow_job": {"labels": ["rise", "unsupported-label"]}}
    with pytest.raises(WebhookError) as exc:
        check_required_labels(payload)
    assert exc.value.status_code == 200
    assert "unsupported labels" in exc.value.message


@patch("redis_client.connect")
@patch("redis_client.enqueue_job", return_value=True)
def test_webhook_queued_enqueues(mock_enqueue, mock_connect):
    """Test that a queued webhook enqueues the job to Redis."""
    from handler import app

    payload = {
        "action": "queued",
        "workflow_job": {"id": 12345, "name": "test", "labels": ["rise", "ubuntu-24.04-riscv"]},
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
        assert b"enqueued" in resp.data
        mock_enqueue.assert_called_once()


@patch("redis_client.connect")
@patch("redis_client.complete_job", return_value="running")
def test_webhook_completed_marks_complete(mock_complete, mock_connect):
    """Test that a completed webhook marks the job as completed in Redis."""
    from handler import app

    payload = {
        "action": "completed",
        "workflow_job": {"id": 12345, "name": "test", "labels": ["rise", "ubuntu-24.04-riscv"]},
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
        mock_complete.assert_called_once()
