import os
import pytest
from unittest.mock import patch, MagicMock
import kubernetes

from runner import (
    RunnerError,
    authenticate_app,
    ensure_runner_group,
    create_jit_runner_config,
    provision_runner,
    delete_pod,
    init_k8s_client,
)

RUNNER_GROUP_NAME = "RISE RISC-V Runners"


@patch("runner.init_ghapp_private_key")
def test_authentication(mock_private_key, requests_mock):
    """Test authentication with mocked private key."""
    mock_jwk = MagicMock()
    mock_private_key.return_value = mock_jwk

    installation_id = 12345
    payload = {
        "installation": {"id": installation_id},
        "repository": {"id": 99999},
    }

    requests_mock.post(f"https://api.github.com/app/installations/{installation_id}/access_tokens",
                       json={"token": "v1.1f699f1069f60xxx"},
                       status_code=201)

    with patch("runner.generate_jwt", return_value="fake-jwt"):
        token = authenticate_app(payload)
    assert token is not None


def test_ensure_runner_group_existing(requests_mock):
    """Test finding an existing runner group."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    payload = {"repository": {"owner": {"login": org_login}}}

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

    group_id = ensure_runner_group(payload, installation_token, RUNNER_GROUP_NAME)
    assert group_id == 42


def test_ensure_runner_group_create(requests_mock):
    """Test creating a runner group when it doesn't exist."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    payload = {"repository": {"owner": {"login": org_login}}}

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

    group_id = ensure_runner_group(payload, installation_token, RUNNER_GROUP_NAME)
    assert group_id == 99


def test_create_jit_runner_config(requests_mock):
    """Test JIT runner config creation."""
    installation_token = "v1.1f699f1069f60xxx"
    org_login = "riseproject-dev"
    runner_group_id = 42
    payload = {
        "repository": {"owner": {"login": org_login}},
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


@patch('runner.init_k8s_client')
@patch('runner.k8s.client.CoreV1Api')
def test_provision_runner_success(mock_core_v1_api, mock_init_client):
    """Test successful runner provisioning with JIT config."""
    mock_api_client = MagicMock()
    mock_init_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance

    jit_config = "base64-encoded-jit-config"
    pod_name = "rise-riscv-runner-workflow-12345"

    payload = {
        "repository": {"full_name": "riseproject-dev/sample"},
        "workflow_job": {"id": 12345, "html_url": "https://github.com/riseproject-dev/sample/actions/runs/1/job/1"},
    }
    result = provision_runner(payload, jit_config, pod_name, "test-runner-image:latest", {"nodeSelector": {}})
    assert "created successfully" in result

    mock_core_v1_api.assert_called_once()
    mock_api_instance.create_namespaced_pod.assert_called_once()

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
    import runner
    runner.init_k8s_client.cache_clear()
    saved = os.environ.pop("K8S_KUBECONFIG", None)
    try:
        with pytest.raises(kubernetes.config.ConfigException):
            provision_runner({}, "jit-config", "test-pod", "img", {})
    finally:
        if saved is not None:
            os.environ["K8S_KUBECONFIG"] = saved
            runner.init_k8s_client.cache_clear()


@patch('runner.init_k8s_client', side_effect=Exception("Test API Error"))
def test_provision_runner_api_exception(mock_init_client):
    """Test runner provisioning failure due to a generic API error."""
    with pytest.raises(Exception) as excinfo:
        provision_runner({}, "jit-config", "test-pod", "img", {})
    assert "Test API Error" == str(excinfo.value)


@patch('runner.init_k8s_client')
@patch('runner.k8s.client.CoreV1Api')
def test_delete_pod_success(mock_core_v1_api, mock_init_client):
    """Test successful deletion of a runner pod."""
    mock_api_client = MagicMock()
    mock_init_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance

    mock_pod = MagicMock()
    mock_pod.metadata.name = "rise-riscv-runner-workflow-12345"

    result = delete_pod(mock_pod)
    assert "deleted successfully" in result
    mock_api_instance.delete_namespaced_pod.assert_called_once_with(
        name="rise-riscv-runner-workflow-12345", namespace="staging"
    )


@patch('runner.init_k8s_client')
@patch('runner.k8s.client.CoreV1Api')
def test_delete_pod_not_found(mock_core_v1_api, mock_init_client):
    """Test deletion when pod is already gone."""
    mock_api_client = MagicMock()
    mock_init_client.return_value = mock_api_client
    mock_api_client.__enter__ = MagicMock(return_value=mock_api_client)
    mock_api_client.__exit__ = MagicMock(return_value=False)

    mock_api_instance = MagicMock()
    mock_core_v1_api.return_value = mock_api_instance
    mock_api_instance.delete_namespaced_pod.side_effect = kubernetes.client.exceptions.ApiException(status=404)

    mock_pod = MagicMock()
    mock_pod.metadata.name = "rise-riscv-runner-workflow-12345"

    result = delete_pod(mock_pod)
    assert "not found" in result
