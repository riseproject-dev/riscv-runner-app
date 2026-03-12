# RISC-V Runner App

A GitHub App that listens for `workflow_job` webhooks and provisions ephemeral RISC-V GitHub Actions runners on Kubernetes using a demand-matching model.

## Usage

[**RISE RISC-V Runner**](https://github.com/apps/rise-risc-v-runner) is a GitHub App that provides ephemeral RISC-V runners for GitHub Actions workflows.

### Installation

1. Install the app on your organization from https://github.com/apps/rise-risc-v-runner.
2. Contact the app administrators to have your organization added to the allowlist.

### Running workflows on RISC-V

Use `runs-on: ubuntu-24.04-riscv` in your workflow:

```yaml
jobs:
  build:
    runs-on: ubuntu-24.04-riscv
    steps:
      - uses: actions/checkout@v4
      - run: uname -m  # riscv64
```

Available platform labels:

| Labels | Board | Description |
|--------|-------|-------------|
| `ubuntu-24.04-riscv` | `scw-em-rv1` | Scaleway EM-RV1 RISC-V |
| `ubuntu-24.04-riscv-2xlarge` | `cloudv10x-pioneer` | Cloud-V-provided hardware with larger number of cores (MILK-V Pioneer) |
| `ubuntu-24.04-riscv-rvv` | `cloudv10x-rvv` | Cloud-V-provided hardware with RVV support |
| `ubuntu-26.04-riscv` | `scw-em-rv1` | Scaleway EM-RV1 RISC-V (Ubuntu 26.04) |
| `ubuntu-26.04-riscv-2xlarge` | `cloudv10x-pioneer` | Cloud-V-provided hardware with larger number of cores (MILK-V Pioneer) (Ubuntu 26.04) |
| `ubuntu-26.04-riscv-rvv` | `cloudv10x-rvv` | Cloud-V-provided hardware with RVV support (Ubuntu 26.04) |

### Requirements

- [Temporary] Your organization must be on the allowlist. Unauthorized organizations are silently ignored.
- Runners are ephemeral -- each runner handles exactly one job and then terminates.

## Architecture

The app uses a **demand matching** model: on one side, workflow_jobs create demand for runners; on the other, k8s workers provide supply. The background worker scales supply to match demand per (org, k8s_pool) pool, with configurable limits per org.

Jobs and workers are not directly linked -- the only relationship is through the org. GitHub makes no direct job-to-runner link; a runner is attached to an org, and the job runs inside that org.

```
GitHub (workflow_job webhook)
  |
  v
Webhook Handler (handler.py)
  |  - Proxies webhooks to staging for staging orgs (prod only)
  |  - Verifies webhook signature
  |  - Validates labels, authorizes org
  |  - Resolves (org_id, labels) -> (k8s_pool, k8s_image)
  |  - Writes job to Redis
  |  - Serves /usage (per-pool jobs and workers)
  |  - NO GitHub API calls, NO k8s calls
  |
  v
Redis (demand + supply state)
  |  - Job hashes: per-job metadata (pending state derived from status field)
  |  - Pool sets: jobs (demand) and workers (supply) per (org, k8s_pool)
  |
  v
Background Worker (worker.py)
  |  - GH reconciliation: sync Redis with GitHub job status
  |  - Pod cleanup: delete Succeeded/Failed pods, remove from worker sets
  |  - Job cleanup: remove completed job hashes older than 5 minutes
  |  - Demand matching: provision runners where demand > supply
  |  - State logging: log per-pool job/worker counts
  |
  v
Kubernetes (runner pods)
```

### Sequence: Queued webhook

```
GitHub -> Handler: workflow_job (action=queued)
Handler: validate signature, labels, org
Handler: match_labels_to_k8s(labels) -> (k8s_pool, k8s_image)
Handler -> Redis: store_job() -> job hash + pool:jobs
Handler: notify queue_event (wake worker)
Handler -> GitHub: 200 OK
```

### Sequence: Worker provisioning

```
Worker: get_pending_jobs() from pool job sets (filter status=pending, sort by created_at)
Worker: for each pending job:
  - get_pool_demand(org_id, k8s_pool) -> (jobs, workers)
  - if jobs <= workers: skip (demand met)
  - if org total workers >= max_workers: skip
  - has_available_slot(node_selector): skip if no capacity
  - authenticate_app(installation_id) -> token
  - ensure_runner_group(org, token) -> group_id
  - create_jit_runner_config(token, group_id, labels, org, name) -> jit_config
  - provision_runner(jit_config, name, image, pool, org_id) -> pod
  - add_worker(org_id, k8s_pool, pod_name)
```

### Sequence: In-progress webhook

```
GitHub -> Handler: workflow_job (action=in_progress)
Handler -> Redis: update_job_running(job_id)
  - Update hash status=running
Handler -> GitHub: 200 OK
```

### Sequence: Completed webhook

```
GitHub -> Handler: workflow_job (action=completed)
Handler -> Redis: complete_job(job_id)
  - SREM from pool:jobs
  - Update hash status=completed
Handler -> GitHub: 200 OK
```

### Sequence: Cancellation

Cancellation is passive. When a job is cancelled on GitHub:
1. The `completed` webhook fires and removes the job from pool:jobs
2. If a worker was already provisioned, it picks up another job in the org or times out
3. GH reconciliation detects stale jobs within ~15s and cleans them up

### Job lifecycle state machine

```
queued webhook      in_progress webhook     completed webhook
    |                       |                       |
    v                       v                       v
 PENDING  ----------->  RUNNING  ----------->  COMPLETED
    |                                               ^
    +-----------------------------------------------+
              completed webhook (before provision)
```

### Worker lifecycle

```
Pod created by worker -> Running -> Succeeded (job done) / Failed (error)
                                         |
                                    cleanup_pods() deletes pod,
                                    removes from pool:workers
```

### Redis schema

All keys are prefixed with `prod:` or `staging:` depending on environment.

| Key | Type | Contents | Purpose |
|-----|------|----------|---------|
| `{env}:job:{job_id}` | HASH | job data | Per-job metadata |
| `{env}:pool:{org_id}:{k8s_pool}:jobs` | SET | job_ids | Demand: pending+running jobs for this pool |
| `{env}:pool:{org_id}:{k8s_pool}:workers` | SET | pod_names | Supply: provisioned pods for this pool |

**Job hash fields**: `job_id`, `org_id`, `org_name`, `repo_full_name`, `installation_id`, `job_labels` (JSON), `k8s_pool`, `k8s_image`, `html_url`, `status` (pending/running/completed), `created_at`

### Demand matching algorithm

```
demand  = SCARD(pool:{org_id}:{k8s_pool}:jobs)      # pending + running jobs
supply  = SCARD(pool:{org_id}:{k8s_pool}:workers)   # provisioned pods
deficit = demand - supply
```

The worker iterates pending jobs in FIFO order. For each job:
1. If `demand <= supply` for its pool: skip (demand already met)
2. If org's total workers across all pools >= `max_workers`: skip
3. If no k8s node capacity for the pool's node selector: skip
4. Otherwise: provision a new runner

### Configuration

Per-org configuration is defined in `ORG_CONFIG` in `constants.py`:

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Organization name (for logging) |
| `max_workers` | int or None | Maximum concurrent workers across all pools. None = unlimited |
| `pre_allocated` | int | Reserved for future use |
| `staging` | bool | If true, webhooks are proxied from prod to staging |

### HTTP routes

| Route | Method | Description |
|-------|--------|-------------|
| `/` | POST | Webhook endpoint for `workflow_job` events |
| `/health` | GET | Health check (returns `ok`) |
| `/usage` | GET | Human-readable view of per-pool jobs and workers |

### Key files

| File | Purpose |
|------|---------|
| `container/constants.py` | Environment configuration, org config |
| `container/handler.py` | Flask webhook handler -- validates requests, writes to Redis |
| `container/worker.py` | Background worker -- GH reconciliation, demand matching, cleanup |
| `container/k8s.py` | Kubernetes pod provisioning, deletion, capacity checks |
| `container/db.py` | Redis pool-based operations |
| `container/github.py` | GitHub API functions (auth, runner groups, JIT config, job status) |
| `container/serve.py` | Entry point -- starts worker thread and Flask server |
| `container/Dockerfile` | Docker image for the Scaleway Container |

### Infrastructure

| Service | Product | Purpose |
|---------|---------|---------|
| App container | Scaleway Container | Webhook handler + background worker (always-on) |
| Job queue | Scaleway Managed Redis | Demand/supply state store |
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
source .venv/bin/activate && PYTHONPATH=container python3 -m pytest
```

Tests mock Redis and Kubernetes -- no live services are required.

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

Runner pods are automatically cleaned up by the background worker when pods reach Succeeded/Failed phase. Stale completed job hashes are removed after 5 minutes.

To manually clean up finished pods:

```bash
kubectl delete pods -l app=rise-riscv-runner --field-selector=status.phase!=Running,status.phase!=Pending,status.phase!=Unknown
```

### Inspect Redis state

```bash
# Check demand for a pool
redis-cli SCARD staging:pool:{org_id}:{k8s_pool}:jobs

# Check supply for a pool
redis-cli SCARD staging:pool:{org_id}:{k8s_pool}:workers

# View a job
redis-cli HGETALL staging:job:{job_id}
```
