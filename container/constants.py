import os

PROD = os.environ["PROD"].lower() == "true"
PROD_URL = os.environ["PROD_URL"]
STAGING_URL = os.environ["STAGING_URL"]

K8S_KUBECONFIG = os.environ["K8S_KUBECONFIG"]
K8S_NAMESPACE = "default" if PROD else "staging"

GHAPP_ID = 2167633  # https://github.com/apps/rise-risc-v-runner
GHAPP_PRIVATE_KEY = os.environ["GHAPP_PRIVATE_KEY"]  # PEM-encoded private key for the GitHub App
GHAPP_WEBHOOK_SECRET = os.environ["GHAPP_WEBHOOK_SECRET"]  # Secret for validating GitHub webhook signatures

REDIS_URL = os.environ["REDIS_URL"]

RUNNER_GROUP_NAME = "RISE RISC-V Runners"

RISEPROJECT_DEV_ORG_ID = 152654596 # github.com/riseproject-dev
PYTORCH_ORG_ID = 21003710 # github.com/pytorch
GGML_ORG_ORG_ID = 134263123 # github.com/ggml-org (for llama.cpp)

ORG_CONFIG = {
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
}

ALLOWED_ORGS = set(ORG_CONFIG.keys())
STAGING_ORGS = {oid for oid, c in ORG_CONFIG.items() if c.get("staging")}

RUNNER_IMAGE_UBUNTU_24_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-24.04-2.331.0@sha256:45e28749c52470b7fb6a788f1b588f770ddb6e7c19b40805d8de3a88ae7ab7b0"
RUNNER_IMAGE_UBUNTU_26_04 = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:ubuntu-26.04-2.331.0@sha256:9b2d6b7e7189defd8c14780af8bfefc26d2663e587c385b92a274d8bfdefb59c"
RUNNER_IMAGE_DIND = "rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/riscv-runner:dind@sha256:44b63facc1abbcc78d0e2c301bb90022bf550a03c5b64d0235f4f276e73a65e2"

