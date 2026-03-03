# RISC-V Runner App

A GitHub App that listens for `workflow_job` webhooks and provisions ephemeral RISC-V GitHub Actions runners on Kubernetes.

## Usage

[**RISE RISC-V Runner**](https://github.com/apps/rise-risc-v-runner) is a GitHub App that provides ephemeral RISC-V runners for GitHub Actions workflows.

### Installation

1. Install the app on your organization from https://github.com/apps/rise-risc-v-runner.
2. Contact the app administrators to have your organization added to the allowlist.

### Running workflows on RISC-V

Use `runs-on: [rise, ubuntu-24.04-riscv]` in your workflow:

```yaml
jobs:
  build:
    runs-on: [rise, ubuntu-24.04-riscv]
    steps:
      - uses: actions/checkout@v4
      - run: uname -m  # riscv64
```

When the workflow is queued, the app automatically provisions a RISC-V runner pod that picks up the job, executes it, and terminates. Jobs that don't include both `rise` and `ubuntu-24.04-riscv` labels are ignored.

### Requirements

- Your organization must be on the allowlist. Unauthorized organizations are silently ignored.
- Workflows must use `runs-on: [rise, ubuntu-24.04-riscv]` — both labels are required.
- Runners are ephemeral — each runner handles exactly one job and then terminates.

## Architecture

```
GitHub (workflow_job webhook)
  |
  v
Scaleway Serverless Container (handler.py)
  |
  ├── Verifies webhook signature
  ├── Checks the event is a "queued" workflow job
  ├── Checks the job has required labels (rise + runner label)
  ├── Authorizes the organization against an allowlist
  ├── Authenticates as the GitHub App to get an installation token
  ├── Creates a runner registration token
  └── Provisions an ephemeral runner pod on Kubernetes
```

The app runs as a Flask server deployed as a Scaleway Serverless Container. When a GitHub Actions workflow is queued in an allowed organization, the app creates a Kubernetes pod running a RISC-V GitHub Actions runner that registers itself, executes the job, and terminates.

Two containers are deployed:
- `gh-webhook` (production) - receives webhooks from the live GitHub App
- `gh-webhook-staging` (staging) - for testing changes before production

## Development

Create a python venv and install dev dependencies:
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-dev.txt
```

Run tests:
```bash
pytest
```

## Deployment

Deployment is handled automatically by GitHub Actions (`.github/workflows/deploy.yml`).

### How it works

1. **Push to `main`** automatically deploys to **production**: runs tests, builds the `:latest` Docker image, pushes it to Scaleway Container Registry, and deploys both containers via `serverless deploy`.
2. **Push to `staging`** automatically deploys to **staging**: same pipeline but builds the `:staging` image instead.
3. **Manual deploy** via the Actions tab: click "Run workflow", select "staging" or "production".

### What to expect

- The CI pipeline runs tests first. If tests fail, deploy is skipped.
- Docker image build and push takes ~1 minute.
- `serverless deploy` takes ~1 minute to update the containers on Scaleway.
- Total pipeline time is ~2-3 minutes.

### GitHub Secrets

The following secrets must be configured in the repository settings (Settings > Secrets and variables > Actions):

| Secret | Description |
|---|---|
| `SCW_SECRET_KEY` | Scaleway API secret key (used for container registry login and serverless deploy) |
| `GHAPP_WEBHOOK_SECRET` | GitHub webhook HMAC secret |
| `GHAPP_PRIVATE_KEY` | GitHub App RSA private key (PEM format) |
| `K8S_KUBECONFIG` | Kubeconfig for the Kubernetes cluster |

## Operations

### Cleanup terminated runner pods

Runner pods are ephemeral and terminate after the job completes. To clean up finished pods:

```bash
kubectl delete pods -l app=rise-riscv-runner --field-selector=status.phase!=Running,status.phase!=Pending,status.phase!=Unknown
```
