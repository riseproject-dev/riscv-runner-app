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

When the workflow is queued, the app enqueues the job and a background worker provisions a RISC-V runner pod when cluster capacity is available. The runner picks up the job, executes it, and terminates. Jobs that don't include both `rise` and `ubuntu-24.04-riscv` labels are ignored.

### Requirements

- Your organization must be on the allowlist. Unauthorized organizations are silently ignored.
- Workflows must use `runs-on: [rise, ubuntu-24.04-riscv]` — both labels are required.
- Runners are ephemeral — each runner handles exactly one job and then terminates.

## Architecture

```
GitHub (workflow_job webhook)
  │
  ▼
Scaleway Container (handler.py)
  │
  ├── Verifies webhook signature
  ├── Checks the event is a "queued" or "completed" workflow job
  ├── Checks the job has required labels (rise + platform label)
  ├── Authorizes the organization against an allowlist
  └── Enqueues job to Redis / marks job completed in Redis
        │
        ▼
      Redis (job queue + state store)
        │
        ▼
Background Worker Thread (worker.py)
  │
  ├── Polls pending jobs from Redis
  ├── Checks k8s cluster capacity per node selector
  ├── Authenticates as GitHub App, creates JIT runner config
  ├── Provisions runner pod on Kubernetes
  ├── Cleans up pods for completed jobs
  └── Reconciles orphan pods
```

### How it works

The app runs as a Flask server with a background worker thread, deployed as a Scaleway Container (always-on). The webhook handler and worker communicate through Redis.

**On `queued` events:** The webhook handler validates the request, checks labels and organization authorization, then enqueues the job to Redis. No GitHub API calls or k8s provisioning happens in the webhook handler — it returns immediately.

**On `completed` events:** The handler marks the job as completed in Redis. If the job was still pending (never provisioned), it's simply removed from the queue. If it was running, the worker will clean up the pod.

**Background worker:** Polls Redis every 10 seconds. For each pending job, it checks whether the k8s cluster has available capacity matching the job's node selector. When capacity exists, it authenticates as the GitHub App, creates a JIT runner config, and provisions the pod. It also cleans up pods for completed jobs and reconciles orphan pods.

### Key files

| File | Purpose |
|------|---------|
| `container/handler.py` | Flask webhook handler — validates requests and enqueues/completes jobs in Redis |
| `container/worker.py` | Background worker thread — provisions pods, cleans up, reconciles |
| `container/runner.py` | GitHub API (auth, runner groups, JIT config) and k8s pod provisioning/deletion |
| `container/redis_client.py` | Redis connection and job queue CRUD operations |
| `container/serve.py` | Entry point — starts the worker thread and Flask server |
| `container/Dockerfile` | Docker image for the Scaleway Container |

### Infrastructure

| Service | Product | Purpose |
|---------|---------|---------|
| App container | Scaleway Container | Webhook handler + background worker (always-on) |
| Job queue | Scaleway Managed Redis | Job queue and state store |
| Runner pods | Self-hosted k8s cluster | Ephemeral RISC-V runner pods |

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
PYTHONPATH=container pytest
```

Tests mock Redis and Kubernetes — no live services are required.

## Deployment

Deployment is handled automatically by GitHub Actions (`.github/workflows/deploy.yml`).

### How it works

1. **Push to `main`** automatically deploys to **production**: runs tests, builds the `:latest` Docker image, pushes it to Scaleway Container Registry, and deploys both containers via `serverless deploy`.
2. **Push to `staging`** automatically deploys to **staging**: same pipeline but builds the `:staging` image instead. After deploy, it triggers a sample workflow to verify end-to-end.
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
| `REDIS_URL` | Redis connection string (e.g. `rediss://default:<password>@<host>:<port>`) |
| `RISCV_RUNNER_SAMPLE_ACCESS_TOKEN` | PAT for triggering sample workflow on staging deploy |

### Kubernetes RBAC

The k8s user `gh-app` needs edit access and permission to list cluster nodes for capacity checks:

```bash
# Create the gh-app user
kubeadm kubeconfig user --client-name=gh-app
# Give the gh-app user edit access
kubectl create clusterrolebinding gh-app-edit-binding --clusterrole=edit --user=gh-app
# Give the gh-app user the ability to list nodes
kubectl create clusterrole gh-app-node-reader --verb=list --resource=nodes
kubectl create clusterrolebinding gh-app-node-reader --clusterrole=gh-app-node-reader --user=gh-app
```

## Operations

### Cleanup terminated runner pods

Runner pods are automatically cleaned up by the background worker when jobs complete. Orphan pods (not tracked in Redis) are also reconciled and removed.

To manually clean up finished pods:

```bash
kubectl delete pods -l app=rise-riscv-runner --field-selector=status.phase!=Running,status.phase!=Pending,status.phase!=Unknown
```
