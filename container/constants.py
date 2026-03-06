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

ORG_CONFIG = {
    152654596: {"name": "riseproject-dev", "max_workers": None, "pre_allocated": 0, "staging": True},
    660779: {"name": "luhenry", "max_workers": 5, "pre_allocated": 0, "staging": False},
}

ALLOWED_ORGS = set(ORG_CONFIG.keys())
STAGING_ORGS = {oid for oid, c in ORG_CONFIG.items() if c.get("staging")}
