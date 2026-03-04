import os
import hmac
import hashlib
import jwt
import json
import pytest
import requests
from unittest.mock import patch, MagicMock
import kubernetes

from handler import (
    ALLOWED_ORGS,
    RUNNER_GROUP_NAME,
    WebhookError,
    check_webhook_signature,
    check_webhook_event,
    authorize_organization,
    authenticate_app_as_organization,
    ensure_runner_group,
    create_jit_runner_config,
    provision_runner,
    delete_runner,
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

def test_authentication(requests_mock):
    app_id = 2167633
    installation_id = 12345
    # This is a sample RSA private key for testing purposes only.
    private_key = """
-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDVr3YJUU7LEeRq
O9Tix1NA3sQ9K4s7NJDAfhyt3znBSNu6ohenQvZTLAGVWA3sqYhH/fPXs/TgyvL+
6YjGWVQthKrTg/c6hnNTRwWcmzOyIsbYF9F573QwogM1B2AAw9X7D4EmVLRTWmRM
rSlolxupa0K5w31f5H6Tgv6thzBX6LFe17b7uoer8qvSHUigyJ2rvZqrabhPxGmH
kXg6MWAiBInMTlIdtX2IJLkCGDvvpFqkLbXMRj0dt/nCQR8I6bSUPTNPbqkpqJRQ
0Ko8B25ju27YGVizy0GeknTURPgxMykVwh5cxU37Ro9Qi8ITYLcYO+CCCoOYlCPW
HwFqZXZhAgMBAAECggEBAMK+toSnZXgNRm7LOKm1n1pvq8lT9gBvV70XMmwEFU7i
Z98f+w6lKHmEkazaI1ac62cxOxpLF9IHJI7Np6mdn+ocDtPWYWslPdWX1LV1fRfM
OgyXKIJIiUwJW4LoxcXstQeqibm1WOLebqqy5ho8HSm6Z4WFdK4AQJuPtyvPGXAD
JnNxCChPsKc54Nuy1OrRRvC14isuCZI1VwUg2Izqv5HTRBBaoiKgc4X6mNA5iipE
4cT1lLV0KVmYNRq1JRknOTFxatBLvncZLqKdcl2rlN10haUBNZ1y+EioFh1BGN3V
VoO+52m1dSLjABH2Ef9FzrNnEDfMzqvOl7DHO8M1SkECgYEA/WSZWqP+6ji5FB7q
5thjhDOsblsQ2k5/56aOMTWCjckPbsWN0Pi1cJD4g86/lrXHH8Kc1QiaeJQgDLV/
LZJsx7BIRkBeD1VzNYnCYwKA3gEB03au7af4T56Usk7E+uHfX/U3Q4PJJ9q+bm6U
sSTxBCOrCd/Ry90hPIq2jTk3I/MCgYEA1+JHKFIk+vXR7+917Z9b5w5ViFciq6sA
cFjIuN3altY8icN7pFYowmTnoqwrehMNqakGyjk+UrVW2fkIKt6avxyxh8MwocWR
lRvnKuxQ7O02IWin0XAvS3GfZwQhvNn/sOFb9/TFtAv+ACZhEe/pXUMzE89KLV/X
STnbVgq0VVsCgYEAh0UvAM5PhWYml3Ex4W5fIfIb+QWwZ3pEmbu2aNqyCVLuZCoe
XRKIecFKicLTUHdWB8RyyN9A52HcAizZ6dAjNi8LRkWScQki6c/S79wkQ1+yQ9s1
4zUqQAbeRpn6Whw+jRFxIR+3QQlrY7SwuCiKabVI14qeiwBPf+xlK9sBbrUCgYAj
Gt+ZVeo/iPOvgY/6qPxH0VPlTM4Nfkwe+MEDFshx2LqVaF1VttD/82qbUEXtnuWM
3jiFb9OLnYNXBKDoX7RoOWFBA2OIGtl2lsf7edwa+uPfgOYxL33xVbOnC8v0qrpi
Z/MNmhcAFScjnRoR0aJwEPpgUUftovUeKjNZhXoXmwKBgQDs5Xbqqan4Bc5pBw/e
wjZZ/5LNtLkZm6Ve/V0X90SbH1DIa2Um+LggWop7FjP9HMbxvdWgNjzvAHy79TAO
XHNCH2WjL0p4gB7VmGgy1U4lAOI6uaTjtosrIzpG+yO7hS0NtqKUQYM8nKuURjZr
+cs5S6dUsqBGIxQpSLhLOu5eSA==
-----END PRIVATE KEY-----
"""
    os.environ["GHAPP_PRIVATE_KEY"] = private_key

    payload = {
        "installation": {"id": installation_id},
        "repository": {"id": 99999},
    }

    requests_mock.post(f"https://api.github.com/app/installations/{installation_id}/access_tokens",
                       json={"token": "v1.1f699f1069f60xxx"},
                       status_code=201)

    token = authenticate_app_as_organization(payload)
    assert token is not None

def test_ensure_runner_group_existing(requests_mock):
    """Test finding an existing runner group."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    payload = {"organization": {"login": org_login}}

    requests_mock.get(
        f"https://api.github.com/orgs/{org_login}/actions/runner-groups",
        json={
            "total_count": 2,
            "runner_groups": [
                {"id": 1, "name": "Default"},
                {"id": 42, "name": RUNNER_GROUP_NAME},
            ]
        },
        status_code=200,
    )

    group_id = ensure_runner_group(payload, installation_token)
    assert group_id == 42

def test_ensure_runner_group_create(requests_mock):
    """Test creating a runner group when it doesn't exist."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    payload = {"organization": {"login": org_login}}

    requests_mock.get(
        f"https://api.github.com/orgs/{org_login}/actions/runner-groups",
        json={"total_count": 1, "runner_groups": [{"id": 1, "name": "Default"}]},
        status_code=200,
    )
    requests_mock.post(
        f"https://api.github.com/orgs/{org_login}/actions/runner-groups",
        json={"id": 99, "name": RUNNER_GROUP_NAME},
        status_code=201,
    )

    group_id = ensure_runner_group(payload, installation_token)
    assert group_id == 99

def test_create_jit_runner_config(requests_mock):
    """Test JIT runner config creation."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    runner_group_id = 42
    payload = {
        "organization": {"login": org_login},
        "workflow_job": {"id": 12345, "labels": ["rise", "ubuntu-24.04-riscv"]},
    }

    requests_mock.post(
        f"https://api.github.com/orgs/{org_login}/actions/runners/generate-jitconfig",
        json={
            "runner": {"id": 23, "name": "test-runner"},
            "encoded_jit_config": "base64-encoded-jit-config-string",
        },
        status_code=201,
    )

    jit_config, pod_name = create_jit_runner_config(payload, installation_token, runner_group_id, ["rise", "ubuntu-24.04-riscv"])
    assert jit_config == "base64-encoded-jit-config-string"
    assert pod_name == "rise-riscv-runner-workflow-12345"

@patch('handler.init_k8s_config', return_value={})
@patch('handler.k8s.config.new_client_from_config_dict')
@patch('handler.k8s.client.CoreV1Api')
def test_provision_runner_success(mock_core_v1_api, mock_create_client, mock_init_config):
    """Test successful runner provisioning with JIT config."""
    mock_api_client = MagicMock()
    mock_create_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance

    jit_config = "base64-encoded-jit-config"
    pod_name = "rise-riscv-runner-workflow-12345-1700000000"
    os.environ["K8S_NAMESPACE"] = "test-namespace"
    os.environ["RUNNER_IMAGE"] = "test-runner-image:latest"

    payload = {
        "repository": {"full_name": "riseproject-dev/sample"},
        "workflow_job": {"html_url": "https://github.com/riseproject-dev/sample/actions/runs/1/job/1"},
    }
    result = provision_runner(payload, jit_config, pod_name, "test-runner-image:latest", {"nodeSelector": {}})
    assert "created successfully" in result

    mock_core_v1_api.assert_called_once()
    mock_api_instance.create_namespaced_pod.assert_called_once()

    # Verify pod manifest details
    call_args = mock_api_instance.create_namespaced_pod.call_args
    pod_manifest = call_args[1]['body']
    assert pod_manifest['metadata']['name'] == pod_name
    assert pod_manifest['spec']['containers'][0]['image'] is not None
    args = pod_manifest['spec']['containers'][0]['args']
    assert len(args) == 1
    assert "--jitconfig" in args[0]
    assert jit_config in args[0]
    assert pod_manifest['spec']['containers'][0]['resources'] == {"limits": {"riseproject.com/runner": "1"}}

def test_provision_runner_config_exception():
    """Test runner provisioning failure due to missing K8S_KUBECONFIG."""
    import handler
    handler.init_k8s_config.cache_clear()
    saved = os.environ.pop("K8S_KUBECONFIG", None)
    try:
        with pytest.raises(kubernetes.config.ConfigException):
            provision_runner({}, "jit-config", "test-pod", "img", {})
    finally:
        if saved is not None:
            os.environ["K8S_KUBECONFIG"] = saved
            handler.init_k8s_config.cache_clear()

@patch('handler.init_k8s_config', return_value={})
@patch('handler.k8s.config.new_client_from_config_dict', side_effect=Exception("Test API Error"))
def test_provision_runner_api_exception(mock_create_client, mock_init_config):
    """Test runner provisioning failure due to a generic API error."""
    with pytest.raises(Exception) as excinfo:
        provision_runner({}, "jit-config", "test-pod", "img", {})
    assert "Test API Error" == str(excinfo.value)

@patch('handler.init_k8s_config', return_value={})
@patch('handler.k8s.config.new_client_from_config_dict')
@patch('handler.k8s.client.CoreV1Api')
def test_delete_runner_success(mock_core_v1_api, mock_create_client, mock_init_config):
    """Test successful deletion of a cancelled runner pod."""
    mock_api_client = MagicMock()
    mock_create_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance

    payload = {
        "workflow_job": {"id": 12345, "conclusion": "cancelled", "html_url": "https://github.com/org/repo/actions/runs/1/job/12345"},
        "repository": {"full_name": "riseproject-dev/sample"},
    }

    result = delete_runner(payload)
    assert "deleted successfully" in result
    mock_api_instance.delete_namespaced_pod.assert_called_once_with(
        name="rise-riscv-runner-workflow-12345", namespace="default"
    )

@patch('handler.init_k8s_config', return_value={})
@patch('handler.k8s.config.new_client_from_config_dict')
@patch('handler.k8s.client.CoreV1Api')
def test_delete_runner_not_found(mock_core_v1_api, mock_create_client, mock_init_config):
    """Test deletion when pod is already gone."""
    mock_api_client = MagicMock()
    mock_create_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance
    mock_api_instance.delete_namespaced_pod.side_effect = kubernetes.client.exceptions.ApiException(status=404)

    payload = {
        "workflow_job": {"id": 12345, "conclusion": "cancelled"},
        "repository": {"full_name": "riseproject-dev/sample"},
    }

    result = delete_runner(payload)
    assert "not found" in result
