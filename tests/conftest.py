import sys
import types

# Mock the constants module before any container module is imported.
# This avoids requiring real env vars (PROD, PROD_URL, etc.) during tests.
mock_constants = types.ModuleType("constants")
mock_constants.PROD = False
mock_constants.PROD_URL = "https://prod.example.com"
mock_constants.STAGING_URL = "https://staging.example.com"
mock_constants.K8S_NAMESPACE = "staging"
mock_constants.GHAPP_ID = 2167633
mock_constants.GHAPP_PRIVATE_KEY = "test-key"
mock_constants.GHAPP_WEBHOOK_SECRET = "test-webhook-secret"
mock_constants.REDIS_URL = "rediss://localhost:6379"
mock_constants.RUNNER_GROUP_NAME = "RISE RISC-V Runners"
mock_constants.K8S_KUBECONFIG = None
mock_constants.RISEPROJECT_DEV_ORG_ID = 152654596
mock_constants.PYTORCH_ORG_ID = 21003710
mock_constants.GGML_ORG_ORG_ID = 134263123
mock_constants.RUNNER_IMAGE_UBUNTU_24_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-24.04-2.331.0"
mock_constants.RUNNER_IMAGE_UBUNTU_26_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-26.04-2.331.0"
mock_constants.RUNNER_IMAGE_DIND = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:dind"
mock_constants.ORG_CONFIG = {
    152654596: {"max_workers": None, "pre_allocated": 0, "staging": True},
    21003710: {"max_workers": 20, "pre_allocated": 0, "staging": False},
    134263123: {"max_workers": 20, "pre_allocated": 0, "staging": False},
}
mock_constants.ALLOWED_ORGS = set(mock_constants.ORG_CONFIG.keys())
mock_constants.STAGING_ORGS = {oid for oid, c in mock_constants.ORG_CONFIG.items() if c.get("staging")}

sys.modules["constants"] = mock_constants
