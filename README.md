# RISC-V Runner App

A GitHub App that listens for `workflow_job` webhooks and provisions ephemeral RISC-V GitHub Actions runners on Kubernetes using a demand-matching model.

## Usage

[**RISE RISC-V Runners**](https://github.com/apps/rise-risc-v-runners) is a GitHub App that provides ephemeral RISC-V runners for GitHub Actions workflows.

### Installation

1. Install the app on your organization from https://github.com/apps/rise-risc-v-runners.
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

- Install the GitHub App on your organization or personal account.
- Runners are ephemeral -- each runner handles exactly one job and then terminates.

## Architecture

The app uses a **demand matching** model: on one side, workflow_jobs create demand for runners; on the other, k8s workers provide supply. The scheduler scales supply to match demand per (entity, k8s_pool) pool, with configurable limits.

Two GitHub Apps are used: one for organizations (org-scoped runners with runner groups) and one for personal accounts (repo-scoped runners). The `entity_id` abstracts over both: it is `org_id` for organizations or `repo_id` for personal accounts.

Jobs and workers are not directly linked -- the only relationship is through the entity. GitHub makes no direct job-to-runner link; a runner is attached to an org or repo, and the job runs inside that context.

The system is split into two containers:
- **gh-webhook** receives GitHub webhooks, validates them, and writes job state to Redis. It makes no GitHub API or k8s calls.
- **scheduler** reads job state from Redis, provisions runner pods on k8s, reconciles with GitHub, and cleans up completed pods.

```
GitHub (workflow_job webhook)
  |
  v
gh-webhook (gh_webhook.py)
  |  - Proxies webhooks to staging for staging entities (prod only)
  |  - Verifies webhook signature
  |  - Validates labels, determines entity type (org or personal)
  |  - Resolves (entity_id, labels) -> (k8s_pool, k8s_image)
  |  - Writes job to Redis, publishes queue_event
  |  - Serves /usage, /history
  |  - NO GitHub API calls, NO k8s calls
  |
  v
Redis (demand + supply state)
  |  - Job hashes: per-job metadata (pending state derived from status field)
  |  - Pool sets: jobs (demand) and workers (supply) per (entity, k8s_pool)
  |  - queue_event pubsub channel: wakes scheduler on new jobs
  |
  v
Scheduler (scheduler.py)
  |  - GH reconciliation: sync Redis with GitHub job status
  |  - Pod cleanup: delete Succeeded/Failed pods, remove from worker sets
  |  - Job cleanup: remove completed job hashes older than 15 days
  |  - Demand matching: provision runners where demand > supply
  |  - State logging: log per-pool job/worker counts
  |  - Woken by Redis pubsub or 15s timeout
  |
  v
Kubernetes (runner pods)
```

### Sequence: Queued webhook

```
GitHub -> gh-webhook: workflow_job (action=queued)
gh-webhook: validate signature, labels, entity type
gh-webhook: match_labels_to_k8s(labels) -> (k8s_pool, k8s_image)
gh-webhook -> Redis: store_job() -> job hash + pool:jobs + publish queue_event
gh-webhook -> GitHub: 200 OK
```

### Sequence: Scheduler provisioning

```
Scheduler: woken by queue_event (or 15s timeout)
Scheduler: get_pending_jobs() from pool job sets (filter status=pending, sort by created_at)
Scheduler: for each pending job:
  - get_pool_demand(entity_id, k8s_pool) -> (jobs, workers)
  - if jobs <= workers: skip (demand met)
  - if entity total workers >= max_workers: skip
  - has_available_slot(node_selector): skip if no capacity
  - authenticate_app(installation_id, entity_type) -> token
  - [org] ensure_runner_group(entity_name, token) -> group_id
  - [org] create_jit_runner_config_org(token, group_id, labels, entity_name, name) -> jit_config
  - [personal] create_jit_runner_config_repo(token, labels, repo_full_name, name) -> jit_config
  - provision_runner(jit_config, name, image, pool, entity_id) -> pod
  - add_worker(entity_id, k8s_pool, pod_name)
```

### Sequence: In-progress webhook

```
GitHub -> gh-webhook: workflow_job (action=in_progress)
gh-webhook -> Redis: update_job_running(job_id)
  - Update hash status=running
gh-webhook -> GitHub: 200 OK
```

### Sequence: Completed webhook

```
GitHub -> gh-webhook: workflow_job (action=completed)
gh-webhook -> Redis: update_job_completed(job_id)
  - SREM from pool:jobs
  - Update hash status=completed
gh-webhook -> GitHub: 200 OK
```

### Sequence: Cancellation

Cancellation is passive. When a job is cancelled on GitHub:
1. The `completed` webhook fires and removes the job from pool:jobs
2. If a worker was already provisioned, it picks up another job or times out
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
Pod created by scheduler -> Running -> Succeeded (job done) / Failed (error)
                                              |
                                         cleanup_pods() deletes pod,
                                         removes from pool:workers
```

### Redis schema

All keys are prefixed with `prod:` or `staging:` depending on environment.

| Key | Type | Contents | Purpose |
|-----|------|----------|---------|
| `{env}:job:{job_id}` | HASH | job data | Per-job metadata |
| `{env}:pool:{entity_id}:{k8s_pool}:jobs` | SET | job_ids | Demand: pending+running jobs for this pool |
| `{env}:pool:{entity_id}:{k8s_pool}:workers` | SET | pod_names | Supply: provisioned pods for this pool |
| `{env}:queue_event` | PUBSUB | job_id | Wakes the scheduler when a new job is stored |

`entity_id` is `org_id` for organizations, or `repo_id` for personal accounts.

**Job hash fields**: `job_id`, `entity_id`, `entity_name`, `entity_type` (Organization/User), `repo_full_name`, `installation_id`, `job_labels` (JSON), `k8s_pool`, `k8s_image`, `html_url`, `status` (pending/running/completed), `created_at`

### Demand matching algorithm

```
demand  = SCARD(pool:{entity_id}:{k8s_pool}:jobs)      # pending + running jobs
supply  = SCARD(pool:{entity_id}:{k8s_pool}:workers)   # provisioned pods
deficit = demand - supply
```

The scheduler iterates pending jobs in FIFO order. For each job:
1. If `demand <= supply` for its pool: skip (demand already met)
2. If entity's total workers across all pools >= `max_workers`: skip
3. If no k8s node capacity for the pool's node selector: skip
4. Otherwise: provision a new runner

### Configuration

Per-entity configuration is defined in `ENTITY_CONFIG` in `constants.py`, keyed by entity ID (org ID or user ID):

| Field | Type | Description |
|-------|------|-------------|
| `max_workers` | int or None | Maximum concurrent workers across all pools. None = unlimited |
| `staging` | bool | If true, webhooks are proxied from prod to staging |

### HTTP routes

**gh-webhook:**

| Route | Method | Description |
|-------|--------|-------------|
| `/` | POST | Webhook endpoint for `workflow_job` events |
| `/health` | GET | Health check (returns `ok`) |
| `/usage` | GET | Human-readable view of per-pool jobs and workers |
| `/history` | GET | Job history sorted by status (pending, running, completed) then creation time |

**scheduler:**

| Route | Method | Description |
|-------|--------|-------------|
| `/health` | GET | Health check (returns `ok`) |

### Key files

| File | Purpose |
|------|---------|
| `container/constants.py` | Environment configuration, entity config, image tags |
| `container/gh_webhook.py` | Flask webhook handler -- validates requests, writes to Redis |
| `container/scheduler.py` | Scheduler -- GH reconciliation, demand matching, cleanup |
| `container/k8s.py` | Kubernetes pod provisioning, deletion, capacity checks |
| `container/db.py` | Redis pool-based operations and pubsub |
| `container/github.py` | GitHub API functions (auth, runner groups, JIT config, job status) |
| `container/Dockerfile.gh_webhook` | Docker image for the gh-webhook container |
| `container/Dockerfile.scheduler` | Docker image for the scheduler container |

### Infrastructure

| Service | Product | Purpose |
|---------|---------|---------|
| gh-webhook | Scaleway Container | Receives webhooks, writes job state to Redis |
| scheduler | Scaleway Container | Demand matching, pod provisioning, cleanup |
| State store | Scaleway Managed Redis | Job and worker state, pubsub for scheduler wake |
| Runner pods | Self-hosted k8s clusters | Ephemeral RISC-V runner pods |

Production and staging each have their own k8s cluster, provisioned via the `scripts/` tooling. Four containers are deployed total:
- `gh-webhook` + `scheduler` (production, `main` branch)
- `gh-webhook` + `scheduler` (staging, `staging` branch)

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

Deployment is handled automatically by GitHub Actions (`.github/workflows/release.yml`).

### How it works

1. **Push to `main`** automatically deploys to **production**: runs tests, builds the `gh-webhook` and `scheduler` Docker images, pushes them to Scaleway Container Registry, and deploys via `serverless deploy`.
2. **Push to `staging`** automatically deploys to **staging**: same pipeline but builds `:staging` tags. After deploy, it triggers a sample workflow to verify end-to-end.
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
| `GHAPP_WEBHOOK_SECRET` | GitHub webhook HMAC secret (shared by both apps) |
| `GHAPP_ORG_PRIVATE_KEY` | GitHub App RSA private key for organizations (PEM format) |
| `GHAPP_PERSONAL_PRIVATE_KEY` | GitHub App RSA private key for personal accounts (PEM format) |
| `K8S_KUBECONFIG` | Kubeconfig for the Kubernetes cluster |
| `REDIS_URL` | Redis connection string (e.g. `rediss://default:<password>@<host>:<port>`) |
| `RISCV_RUNNER_SAMPLE_ACCESS_TOKEN` | PAT for triggering sample workflow on staging deploy |

## Kubernetes cluster provisioning

Production and staging each have their own k8s cluster on Scaleway, managed via scripts in `scripts/`.

### Provisioning scripts

| Script | Purpose |
|--------|---------|
| `scripts/scw-provision-control-plane.py` | Create a k8s control plane instance (Scaleway POP2-2C-8G) with containerd, kubeadm, Flannel CNI, RBAC, and device plugins |
| `scripts/scw-provision-runner.py` | Create, reinstall, list, or delete bare metal RISC-V runner nodes (Scaleway EM-RV1) |
| `scripts/constants.py` | Scaleway project ID, zone, private network ID, SSH key IDs |
| `scripts/utils.py` | Scaleway SDK clients, SSH helpers, BareMetal/Instance wrappers |

### Creating a new cluster from scratch

```bash
cd scripts
python3 -m venv .venv-scripts
source .venv-scripts/bin/activate
pip3 install -r requirements.txt

# 1. Create the control plane
## Pass --staging for a staging control-plane
python scw-provision-control-plane.py create [--staging]

# 2. Add runner nodes (creates 3 bare metal RISC-V servers)
python scw-provision-runner.py --control-plane <control-plane-name> create 3

# 3. Update Github Secrets:
## Note the `--env main` for the prod environment, use `--env staging` for staging environment
ssh root@$(scw instance server list zone=fr-par-2 project-id=03a2e06e-e7c1-45a6-9f05-775d813c2e28 -o json | jq -r '.[] | select(.name == "<control-plane-name>") | .public_ip.address') cat /etc/kubernetes/kubeconfig-gh-app.conf | gh secret set K8S_KUBECONFIG --repo riseproject-dev/riscv-runner-app --env main
ssh root@$(scw instance server list zone=fr-par-2 project-id=03a2e06e-e7c1-45a6-9f05-775d813c2e28 -o json | jq -r '.[] | select(.name == "<control-plane-name>") | .public_ip.address') cat /etc/kubernetes/kubeconfig-gh-deploy.conf | gh secret set K8S_KUBECONFIG --repo riseproject-dev/riscv-runner-images --env main
ssh root@$(scw instance server list zone=fr-par-2 project-id=03a2e06e-e7c1-45a6-9f05-775d813c2e28 -o json | jq -r '.[] | select(.name == "<control-plane-name>") | .public_ip.address') cat /etc/kubernetes/kubeconfig-gh-deploy.conf | gh secret set K8S_KUBECONFIG --repo riseproject-dev/riscv-runner-device-plugin --env main
```

### Managing runners

```bash
# List runners tagged to a control plane
python scw-provision-runner.py --control-plane <control-plane-name> list

# Reinstall OS on a runner (wipes and re-joins the cluster)
python scw-provision-runner.py --control-plane <control-plane-name> reinstall <runner-name>

# Delete runners
python scw-provision-runner.py --control-plane <control-plane-name> delete <runner-name>
```

### Kubernetes RBAC

RBAC is configured automatically by the control plane provisioning script. The key users:

- `gh-app` -- used by the scheduler container. Has edit access and node list permission for capacity checks.
- `gh-deploy` -- used by CI for kubeconfig stored in GitHub Secrets. Has cluster-admin access.

## Operations

### Cleanup terminated runner pods

Runner pods are automatically cleaned up by the scheduler when pods reach Succeeded/Failed phase. Stale completed job hashes are removed after 15 days.

To manually clean up finished pods:

```bash
kubectl delete pods -l app=rise-riscv-runner --field-selector=status.phase!=Running,status.phase!=Pending,status.phase!=Unknown
```

### Inspect Redis state

```bash
# Check demand for a pool (entity_id = org_id for orgs, repo_id for personal)
redis-cli SCARD staging:pool:{entity_id}:{k8s_pool}:jobs

# Check supply for a pool
redis-cli SCARD staging:pool:{entity_id}:{k8s_pool}:workers

# View a job
redis-cli HGETALL staging:job:{job_id}
```
