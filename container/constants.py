import os
from enum import Enum

class EntityType(str, Enum):
    ORGANIZATION = "Organization"
    USER = "User"

PROD = os.environ["PROD"].lower() == "true"
PROD_URL = os.environ["PROD_URL"]
STAGING_URL = os.environ["STAGING_URL"]

K8S_KUBECONFIG = os.environ["K8S_KUBECONFIG"]
K8S_NAMESPACE = "default" if PROD else "staging"

GHAPP_ORG_ID = 2167633  # https://github.com/apps/rise-risc-v-runner
GHAPP_ORG_PRIVATE_KEY = os.environ["GHAPP_ORG_PRIVATE_KEY"]  # PEM-encoded private key for the org GitHub App
GHAPP_PERSONAL_ID = 3131217  # https://github.com/apps/rise-risc-v-runners-personal
GHAPP_PERSONAL_PRIVATE_KEY = os.environ["GHAPP_PERSONAL_PRIVATE_KEY"]  # PEM-encoded private key for the personal GitHub App
GHAPP_WEBHOOK_SECRET = os.environ["GHAPP_WEBHOOK_SECRET"]  # Secret for validating GitHub webhook signatures

REDIS_URL = os.environ["REDIS_URL"]

RUNNER_GROUP_NAME = "RISE RISC-V Runners"

# gh api orgs/<orgname> --jq '.id'
RISEPROJECT_DEV_ORG_ID = 152654596 # github.com/riseproject-dev
PYTORCH_ORG_ID = 21003710 # github.com/pytorch
GGML_ORG_ORG_ID = 134263123 # github.com/ggml-org (for llama.cpp)
# gh api users/<username> --jq '.id'
LUHENRY_USER_ID = 660779 # github.com/luhenry

ENTITY_CONFIG = {
    RISEPROJECT_DEV_ORG_ID: {
        "max_workers": None,
        "pre_allocated": 0,
        "staging": True,
    },
    PYTORCH_ORG_ID: {
        "max_workers": 20,
        "pre_allocated": 0,
        "staging": False,
    },
    GGML_ORG_ORG_ID: {
        "max_workers": 20,
        "pre_allocated": 0,
        "staging": False,
    },
    LUHENRY_USER_ID: {
        "max_workers": None,
        "pre_allocated": 0,
        "staging": True,
    },
}

STAGING_ENTITIES = {oid for oid, c in ENTITY_CONFIG.items() if c.get("staging")}

RUNNER_IMAGE_UBUNTU_24_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-24.04-2.331.0"
RUNNER_IMAGE_UBUNTU_26_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-26.04-2.331.0"
RUNNER_IMAGE_DIND = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:dind"

