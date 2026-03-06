import pytest
from unittest.mock import patch, MagicMock

from github import (
    GitHubAPIError,
    authenticate_app,
    ensure_runner_group,
    create_jit_runner_config,
    get_job_status,
)


# --- Authentication ---

@patch("github.init_ghapp_private_key")
def test_authenticate_app(mock_private_key, requests_mock):
    mock_private_key.return_value = MagicMock()

    requests_mock.post(
        "https://api.github.com/app/installations/12345/access_tokens",
        json={"token": "v1.test-token"},
        status_code=201,
    )

    with patch("github.generate_jwt", return_value="fake-jwt"):
        token = authenticate_app(12345)
    assert token == "v1.test-token"


@patch("github.init_ghapp_private_key")
def test_authenticate_app_failure(mock_private_key, requests_mock):
    mock_private_key.return_value = MagicMock()

    requests_mock.post(
        "https://api.github.com/app/installations/12345/access_tokens",
        json={"message": "Bad credentials"},
        status_code=401,
    )

    with patch("github.generate_jwt", return_value="fake-jwt"):
        with pytest.raises(GitHubAPIError) as exc:
            authenticate_app(12345)
    assert exc.value.status_code == 401


# --- Runner groups ---

def test_ensure_runner_group_existing(requests_mock):
    requests_mock.get(
        "https://api.github.com/orgs/test-org/actions/runner-groups",
        json={
            "total_count": 2,
            "runner_groups": [
                {"id": 1, "name": "Default"},
                {"id": 42, "name": "RISE RISC-V Runners"},
            ]
        },
    )

    group_id = ensure_runner_group("test-org", "token", "RISE RISC-V Runners")
    assert group_id == 42


def test_ensure_runner_group_creates(requests_mock):
    requests_mock.get(
        "https://api.github.com/orgs/test-org/actions/runner-groups",
        json={"total_count": 1, "runner_groups": [{"id": 1, "name": "Default"}]},
    )
    requests_mock.post(
        "https://api.github.com/orgs/test-org/actions/runner-groups",
        json={"id": 99, "name": "RISE RISC-V Runners"},
        status_code=201,
    )

    group_id = ensure_runner_group("test-org", "token", "RISE RISC-V Runners")
    assert group_id == 99


# --- JIT runner config ---

def test_create_jit_runner_config(requests_mock):
    requests_mock.post(
        "https://api.github.com/orgs/test-org/actions/runners/generate-jitconfig",
        json={
            "runner": {"id": 23, "name": "test-runner"},
            "encoded_jit_config": "base64-jit-config",
        },
        status_code=201,
    )

    jit_config = create_jit_runner_config(
        "token", 42, ["ubuntu-24.04-riscv"], "test-org", "runner-1"
    )
    assert jit_config == "base64-jit-config"


def test_create_jit_runner_config_failure(requests_mock):
    requests_mock.post(
        "https://api.github.com/orgs/test-org/actions/runners/generate-jitconfig",
        json={"message": "Conflict"},
        status_code=409,
    )

    with pytest.raises(GitHubAPIError):
        create_jit_runner_config("token", 42, ["ubuntu-24.04-riscv"], "test-org", "runner-1")


# --- Job status ---

def test_get_job_status(requests_mock):
    requests_mock.get(
        "https://api.github.com/repos/org/repo/actions/jobs/111",
        json={"status": "completed"},
    )

    status = get_job_status("org/repo", 111, "token")
    assert status == "completed"


def test_get_job_status_failure(requests_mock):
    requests_mock.get(
        "https://api.github.com/repos/org/repo/actions/jobs/111",
        text="Not Found",
        status_code=404,
    )

    with pytest.raises(GitHubAPIError):
        get_job_status("org/repo", 111, "token")
