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
    check_webhook_signature,
    check_webhook_event,
    authorize_organization,
    authenticate_app_as_organization,
    create_runner_registration_token,
    provision_runner,
    compute_signature,
)

def test_valid_signature():
    secret = "abcdefghi01234"
    body = '{"action":"queued"}'
    expected_signature = compute_signature(body, secret).hexdigest()
    os.environ["GITHUB_WEBHOOK_SECRET"] = secret
    headers = { "X-Hub-Signature-256": f"sha256={expected_signature}" }

    _, err = check_webhook_signature(headers, body)
    assert err is None

def test_missing_secret():
    if "GITHUB_WEBHOOK_SECRET" in os.environ:
        del os.environ["GITHUB_WEBHOOK_SECRET"]

    _, err = check_webhook_signature({}, "")
    assert err["statusCode"] == 500

def test_invalid_signature():
    os.environ["GITHUB_WEBHOOK_SECRET"] = "secret"
    headers = { "X-Hub-Signature-256": "sha256=invalid" }
    _, err = check_webhook_signature(headers, "")
    assert err["statusCode"] == 401

def test_queued_event():
    body = '{"action":"queued"}'
    payload, err = check_webhook_event(body)
    assert err is None
    assert payload["action"] == "queued"

def test_ignored_event():
    body = '{"action":"completed"}'
    _, err = check_webhook_event(body)
    assert err["statusCode"] == 200
    assert "Ignoring action" in err["body"]

def test_invalid_json():
    _, err = check_webhook_event("{")
    assert err["statusCode"] == 400

def test_authorized_user():
    org_id = list(ALLOWED_ORGS)[0]
    payload = {"organization": {"id": org_id}}
    _, err = authorize_organization(payload)
    assert err is None

def test_unauthorized_user():
    payload = {"organization": {"id": 1}}
    _, err = authorize_organization(payload)
    assert err["statusCode"] == 200
    assert "not authorized" in err["body"]

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
    os.environ["GITHUB_APP_PRIVATE_KEY"] = private_key

    payload = {"installation": {"id": installation_id}}

    requests_mock.post(f"https://api.github.com/app/installations/{installation_id}/access_tokens",
                       json={"token": "v1.1f699f1069f60xxx"},
                       status_code=201)

    token, err = authenticate_app_as_organization(payload)
    assert err is None
    assert token is not None

def test_create_runner_token(requests_mock):
    """Test the runner token creation."""
    installation_token = "v1.1f699f1069f60xxx"

    org_login = "riseproject-dev"
    payload = {"organization": {"login": org_login}}

    requests_mock.post(f"https://api.github.com/orgs/{org_login}/actions/runners/registration-token",
                       json={"token": "runner-token"},
                       status_code=201)

    runner_token, err = create_runner_registration_token(payload, installation_token)
    assert err is None
    assert runner_token == "runner-token"

@patch('handler.k8s.config.load_incluster_config')
@patch('handler.k8s.client.CoreV1Api')
def test_provision_runner_success(mock_core_v1_api, mock_load_incluster_config):
    """Test successful runner provisioning."""
    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance

    runner_token = "test-runner-token"
    payload = {
        "repository": {"html_url": "https://github.com/test/repo"},
        "workflow_job": {"id": 12345}
    }
    os.environ["K8S_NAMESPACE"] = "test-namespace"
    os.environ["RUNNER_IMAGE"] = "test-runner-image:latest"
    os.environ["K8S_API_SERVER"] = "test-k8s-api-server"
    os.environ["K8S_API_TOKEN"] = "test-k8s-api-token"

    _ = provision_runner(payload, runner_token)

    mock_core_v1_api.assert_called_once()
    mock_api_instance.create_namespaced_pod.assert_called_once()

    # Verify pod manifest details
    call_args = mock_api_instance.create_namespaced_pod.call_args
    pod_manifest = call_args[1]['body']
    assert pod_manifest['metadata']['name'].startswith('rise-riscv-runner-12345-')
    assert pod_manifest['spec']['containers'][0]['image'] is not None
    assert pod_manifest['spec']['containers'][0]['command'] is not None
    assert pod_manifest['spec']['containers'][0]['args'] is not None
    assert len(pod_manifest['spec']['containers'][0]['args']) > 0

def test_provision_runner_config_exception():
    """Test runner provisioning failure due to Kubernetes config error."""
    if "K8S_API_SERVER" in os.environ: del os.environ["K8S_API_SERVER"]
    if "K8S_API_TOKEN" in os.environ: del os.environ["K8S_API_TOKEN"]
    with pytest.raises(kubernetes.config.ConfigException):
        _ = provision_runner({}, "token")

@patch('handler.k8s.client.CoreV1Api', side_effect=Exception("Test API Error"))
def test_provision_runner_api_exception(mock_core_v1_api):
    """Test runner provisioning failure due to a generic API error."""
    os.environ["K8S_API_SERVER"] = "test-k8s-api-server"
    os.environ["K8S_API_TOKEN"] = "test-k8s-api-token"
    with pytest.raises(Exception) as excinfo:
        _ = provision_runner({"repository": {"html_url": "url"}, "workflow_job": {"id": 1}}, "token")
    assert "Test API Error" == str(excinfo.value)
