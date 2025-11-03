
import platform

print(f"Current platform: {platform.platform()}")

import hashlib
import hmac
import json
import jwt
import os
import requests
import sys
import time

from flask import Flask, request, make_response
app = Flask(__name__)

# --- 3. Authorize the User (Access Control) ---
# This is the allowlist of GitHub organization IDs that are authorized to use this runner.
# Replace these with the actual organization IDs you want to allow.
ALLOWED_ORGS = {
    152654596, # riseproject-dev
}

def compute_signature(body, secret):
    return hmac.new(secret.encode('utf-8'), msg=body.encode('utf-8'), digestmod=hashlib.sha256)

def verify_signature(body, signature, secret):
    """Verify that the body was sent from GitHub by validating the signature."""
    if not signature:
        return False, "X-Hub-Signature-256 header is missing!"

    hash = compute_signature(body, secret)
    expected_signature = "sha256=" + hash.hexdigest()

    if not hmac.compare_digest(expected_signature, signature):
        return False, f"Request signatures didn't match! Expected: {expected_signature}, Got: {signature}"

    return True, "Signatures match"

def generate_jwt(app_id, private_key):
    """Generate a JWT for GitHub App authentication."""
    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + (10 * 60),  # 10 minutes expiration
        "iss": app_id,
    }
    return jwt.JWT().encode(payload, private_key, alg="RS256")

def get_installation_access_token(jwt_token, installation_id):
    """Get an installation access token from GitHub."""
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)

    if response.status_code == 201:
        return response.json().get("token"), None
    else:
        return None, response.json().get("message", "Failed to get installation token")


def check_webhook_signature(headers, body):
    """Verify the webhook signature."""
    secret = os.environ.get("GITHUB_WEBHOOK_SECRET")
    if not secret:
        return None, {"statusCode": 500, "body": "GITHUB_WEBHOOK_SECRET is not configured."}

    signature = headers.get("X-Hub-Signature-256")
    is_valid, message = verify_signature(body, signature, secret)

    if not is_valid:
        return None, {"statusCode": 401, "body": message}

    return body, None

def check_webhook_event(body):
    """Check if the event is a 'queued' workflow_job."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None, {"statusCode": 400, "body": "Invalid JSON payload"}

    if payload.get("action") != "queued":
        return None, {
            "statusCode": 200,
            "body": f"Ignoring action: {payload.get('action')}",
        }

    return payload, None

def authorize_organization(payload):
    """Authorize the organization."""
    org_id = payload.get("organization", {}).get("id")
    if not org_id:
        return None, {"statusCode": 400, "body": "Missing organization ID in payload"}

    if org_id not in ALLOWED_ORGS:
        return None, {
            "statusCode": 200,
            "body": f"Organization {org_id} not authorized.",
        }

    return org_id, None

def authenticate_app_as_organization(payload):
    """Authenticate the app as the organization and get an installation token."""

    private_key = os.environ.get("GITHUB_APP_PRIVATE_KEY")
    if not private_key:
        return None, {
            "statusCode": 500,
            "body": "GITHUB_APP_PRIVATE_KEY is not configured.",
        }

    app_id = 2167633 # https://github.com/apps/rise-risc-v-runner
    private_key = jwt.jwk_from_pem(private_key.encode('utf-8'))

    if not private_key:
        return None, {
            "statusCode": 500,
            "body": "GITHUB_APP_PRIVATE_KEY is not a valid PEM file.",
        }

    installation_id = payload.get("installation", {}).get("id")
    if not installation_id:
        return None, {"statusCode": 400, "body": "Missing installation ID in payload"}

    jwt_token = generate_jwt(app_id, private_key)
    token, error = get_installation_access_token(jwt_token, installation_id)

    if error:
        return None, {"statusCode": 500, "body": error}

    return token, None

def create_runner_registration_token(payload, installation_token):
    """Create a registration token for a new runner."""
    org_login = payload.get("organization", {}).get("login")
    if not org_login:
        return {"statusCode": 400, "body": "Missing organization login in payload"}

    headers = {
        "Authorization": f"token {installation_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/orgs/{org_login}/actions/runners/registration-token"
    response = requests.post(url, headers=headers)

    if response.status_code == 201:
        return response.json().get("token"), None
    else:
        return None, response.json().get("message", "Failed to create runner registration token")

@app.route("/", methods=['POST'])
def webhook():
    body, err = check_webhook_signature(request.headers, request.get_data(as_text=True))
    if err:
        return make_response(err["body"], err["statusCode"])

    payload, err = check_webhook_event(body)
    if err:
        return make_response(err["body"], err["statusCode"])

    _, err = authorize_organization(payload)
    if err:
        return make_response(err["body"], err["statusCode"])

    installation_token, err = authenticate_app_as_organization(payload)
    if err:
        return make_response(err["body"], err["statusCode"])

    runner_token, err = create_runner_registration_token(payload, installation_token)
    if err:
        return make_response(err["body"], err["statusCode"])

    return "Successfully authenticated and created runner token"

####################################################################################################
####################################################################################################
## Testing Code
####################################################################################################

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
