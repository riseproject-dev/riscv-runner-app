import os
import hmac
import hashlib
import json
import pytest
from unittest.mock import patch, MagicMock

os.environ.setdefault("PROD", "false")
os.environ.setdefault("PROD_URL", "https://prod.example.com")
os.environ.setdefault("STAGING_URL", "https://staging.example.com")

from handler import (
    ALLOWED_ORGS,
    WebhookError,
    check_webhook_signature,
    check_webhook_event,
    check_required_labels,
    authorize_organization,
    compute_signature,
)


def test_valid_signature():
    secret = "abcdefghi01234"
    body = '{"action":"queued"}'
    expected_signature = compute_signature(body, secret).hexdigest()
    os.environ["GHAPP_WEBHOOK_SECRET"] = secret
    headers = { "X-Hub-Signature-256": f"sha256={expected_signature}" }

    result = check_webhook_signature(headers, body)
    assert result == body

def test_missing_secret():
    if "GHAPP_WEBHOOK_SECRET" in os.environ:
        del os.environ["GHAPP_WEBHOOK_SECRET"]

    with pytest.raises(WebhookError) as exc:
        check_webhook_signature({}, "")
    assert exc.value.status_code == 500

def test_invalid_signature():
    os.environ["GHAPP_WEBHOOK_SECRET"] = "secret"
    headers = { "X-Hub-Signature-256": "sha256=invalid" }
    with pytest.raises(WebhookError) as exc:
        check_webhook_signature(headers, "")
    assert exc.value.status_code == 401

def test_queued_event():
    body = '{"action":"queued"}'
    payload, action = check_webhook_event(body)
    assert action == "queued"
    assert payload["action"] == "queued"

def test_completed_event():
    body = '{"action":"completed","workflow_job":{"id":123}}'
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
    payload = {"organization": {"id": org_id}}
    result = authorize_organization(payload)
    assert result == org_id

def test_unauthorized_user():
    payload = {"organization": {"id": 1}}
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

    secret = "test-secret"
    os.environ["GHAPP_WEBHOOK_SECRET"] = secret

    payload = {
        "action": "queued",
        "organization": {"id": list(ALLOWED_ORGS)[0], "login": "riseproject-dev"},
        "workflow_job": {"id": 12345, "name": "test", "labels": ["rise", "ubuntu-24.04-riscv"]},
        "repository": {"full_name": "riseproject-dev/sample"},
    }
    body = json.dumps(payload)
    sig = "sha256=" + compute_signature(body, secret).hexdigest()

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

    secret = "test-secret"
    os.environ["GHAPP_WEBHOOK_SECRET"] = secret

    payload = {
        "action": "completed",
        "organization": {"id": list(ALLOWED_ORGS)[0], "login": "riseproject-dev"},
        "workflow_job": {"id": 12345, "name": "test", "labels": ["rise", "ubuntu-24.04-riscv"]},
        "repository": {"full_name": "riseproject-dev/sample"},
    }
    body = json.dumps(payload)
    sig = "sha256=" + compute_signature(body, secret).hexdigest()

    mock_connect.return_value = MagicMock()

    with app.test_client() as client:
        resp = client.post("/", data=body, headers={
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        })
        assert resp.status_code == 200
        assert b"completed" in resp.data
        mock_complete.assert_called_once()
