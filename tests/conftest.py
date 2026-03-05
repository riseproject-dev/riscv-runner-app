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

sys.modules["constants"] = mock_constants
