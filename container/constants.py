import os

PROD = os.environ["PROD"].lower() == "true"
PROD_URL = os.environ["PROD_URL"]
STAGING_URL = os.environ["STAGING_URL"]

K8S_NAMESPACE = "default" if PROD else "staging"

GHAPP_ID = 2167633  # https://github.com/apps/rise-risc-v-runner
GHAPP_PRIVATE_KEY = os.environ["GHAPP_PRIVATE_KEY"]  # PEM-encoded private key for the GitHub App
GHAPP_WEBHOOK_SECRET = os.environ["GHAPP_WEBHOOK_SECRET"]  # Secret for validating GitHub webhook signatures

REDIS_URL = os.environ["REDIS_URL"]