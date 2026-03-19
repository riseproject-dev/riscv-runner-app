# Re-Architecture: Job Lifecycle → Demand Matching

## Context

The current architecture treats each workflow_job as an independent lifecycle (handler generates JIT config, enqueues ready-to-provision job, worker creates pod). This tightly couples the webhook handler to GitHub API calls and makes it hard to reason about per-org capacity.

The new model reframes this as **demand matching**: on one side we have workflow_jobs needing runners (demand), on the other we have k8s workers (supply). The background worker scales supply to match demand per (org, platform) pool, with configurable limits. **Jobs and workers are not directly linked** — the only relationship is through the org. GitHub makes no direct job-to-runner link; a runner is attached to an org, and the job runs inside that org.

## Key Decisions

- **Event type**: `workflow_job` (not `workflow_run`)
- **Demand unit**: Per job, per (org, k8s_pool) pool
- **JIT config**: Generated in the worker at provisioning time
- **Handler scope**: Validation + label→k8s resolution + Redis write. NO GitHub API calls, NO k8s calls.
- **Handler actions**: `queued`, `in_progress`, `completed`
- **No job-to-worker link**: Pods have no `job_id` label. Job hashes have no `pod_name`. Workers are org capacity.
- **Cancel handling**: Passive — excess workers pick up next job or eventually time out
- **Pod timeout**: FIXME — JIT runners hang forever. TBD.
- **Pod cleanup**: By phase (Succeeded/Failed) + `app=rise-riscv-runner` label
- **Org config**: `ORG_CONFIG` dict in `constants.py`. `ALLOWED_ORGS`/`STAGING_ORGS` derived from it.
- **Redis schema**: Per (org, k8s_pool) pools with `jobs` (demand) and `workers` (supply) sets
- **k8s_pool**: Board name string (e.g., `scw-em-rv1`, `cloudv10x-rvv`) from `match_labels_to_k8s`. Used as Redis pool key and pod label `riseproject.com/board`.
- **max_workers**: Global across all pools for an org (not per-pool)
- **Capacity check**: Worker checks `has_available_slot()` before provisioning
- **Worker loop**: Single thread — GH reconciliation, then demand matching, then cleanup. ~15s interval.

## Redis Schema

**Key structure** (all prefixed with `prod:` or `staging:`):

| Key | Type | Contents | Purpose |
|-----|------|----------|---------|
| `{env}:job:{job_id}` | HASH | job data | Per-job metadata |
| `{env}:pool:{org_id}:{k8s_pool}:jobs` | SET | job_ids | Demand: pending+running jobs for this pool |
| `{env}:pool:{org_id}:{k8s_pool}:workers` | SET | pod_names | Supply: provisioned pods for this pool |
| `{env}:orgs` | SET | org_ids | All orgs with tracked jobs |
| `{env}:pending` | ZSET | job_ids scored by created_at | Global FIFO queue of all pending jobs |

Where `k8s_pool` is the board name (e.g., `scw-em-rv1`, `cloudv10x-rvv`) returned by `match_labels_to_k8s`.

**Job HASH fields**: `job_id`, `org_id`, `org_name`, `repo_full_name`, `installation_id`, `labels` (JSON), `k8s_pool` (board name, e.g. `scw-em-rv1`), `k8s_image`, `status` (pending/running/completed), `created_at`

**Demand matching formula**:
```
demand  = SCARD(pool:{org_id}:{k8s_pool}:jobs)      # pending + running jobs
supply  = SCARD(pool:{org_id}:{k8s_pool}:workers)   # provisioned pods
deficit = demand - supply
```

**Lifecycle**:
- `queued` webhook → create job hash (status=pending), SADD job_id to `pool:jobs`, ZADD to `pending`, SADD org to `orgs`
- Worker provisions → SADD pod_name to `pool:workers`, ZREM from `pending`, update job hash status=running
- `in_progress` webhook → update job hash status to running
- `completed` webhook → SREM job_id from `pool:jobs`, ZREM from `pending` (if still there), update job hash status=completed
- Worker cleanup → list pods in Succeeded/Failed phase, delete pod, SREM pod_name from `pool:workers`

## Implementation Plan

### 1. `container/constants.py`

- Add `ORG_CONFIG`:
  ```python
  ORG_CONFIG = {
      152654596: {"name": "riseproject-dev", "max_workers": None, "pre_allocated": 0, "staging": True},
      660779: {"name": "luhenry", "max_workers": 5, "pre_allocated": 0, "staging": False},
  }
  ALLOWED_ORGS = set(ORG_CONFIG.keys())
  STAGING_ORGS = {oid for oid, c in ORG_CONFIG.items() if c.get("staging")}
  ```
- Move `RUNNER_GROUP_NAME` here

### 2. `container/github.py` — NEW file

Extract GitHub API functions from `handler.py`:
- `GitHubAPIError(status_code, message)` exception
- `init_ghapp_private_key()` — cached
- `generate_jwt(app_id, private_key)`
- `authenticate_app(installation_id)`
- `ensure_runner_group(org_name, token, group_name)` → group_id
- `create_jit_runner_config(token, group_id, labels, org_name, runner_name)` → jit_config
- `get_job_status(repo_full_name, job_id, token)` → status string — for reconciliation

### 3. `container/db.py` — Rewrite with pool-based schema

**Handler operations:**
- `store_job(job_id, org_id, org_name, repo_full_name, installation_id, labels, k8s_pool, k8s_image)` → bool
  - HSETNX for idempotency, SADD to `pool:jobs`, ZADD to `pending` (scored by created_at), SADD org to `orgs`, notify `queue_event`
- `update_job_running(job_id)` → prev_status
  - Update hash status to running (no set changes)
- `complete_job(job_id)` → prev_status
  - SREM from `pool:jobs`, ZREM from `pending`, update hash status=completed

**Worker operations:**=
- `get_pool_demand(org_id, k8s_pool)` → (job_count, worker_count)
- `get_total_workers_for_org(org_id)` → total worker count across all pools (for max_workers check)
- `get_pending_jobs()` → list of job_ids from `{env}:pending` ZSET in FIFO order (ZRANGE)
- `mark_provisioned(job_id, pod_name)` → ZREM from `pending`, update job hash status=running
- `add_worker(org_id, k8s_pool, pod_name)` → SADD to `pool:workers`
- `remove_worker(org_id, k8s_pool, pod_name)` → SREM from `pool:workers`
- `get_job(job_id)` → dict
- `cleanup_job(job_id)` → delete hash only (worker set managed separately)
- `get_all_job_ids()` → all job_ids across all pool:jobs sets

**Threading**: `queue_event = threading.Condition()`

### 4. `container/handler.py` — Simplify

**Remove**: All GitHub API functions, `ALLOWED_ORGS`/`STAGING_ORGS` definitions, `jwt` import

**Keep/modify**:
- Webhook signature verification (unchanged)
- `check_webhook_event()`: Accept `queued`, `in_progress`, `completed`
- `authorize_organization()`: Import `ALLOWED_ORGS` from constants
- `match_labels_to_k8s(labels)`: Keep here, returns `(k8s_pool, k8s_image)`
  - `k8s_pool` is the board name (e.g., `"scw-em-rv1"`) used as Redis pool key
  - Worker reconstructs nodeSelector as `{"riseproject.dev/board": k8s_pool}`

**`webhook()` route:**
- `queued` → validate labels, `match_labels_to_k8s()` → (k8s_pool, k8s_image), extract fields, `db.store_job()`
- `in_progress` → `db.update_job_running()`
- `completed` → `db.complete_job()`, return 200 even if not found (no k8s fallback)

### 5. `container/k8s.py` — Updates

- Change `provision_runner(jit_config, runner_name, k8s_image, k8s_pool, org_id)` signature
  - Reconstruct nodeSelector internally: `{"riseproject.dev/board": k8s_pool}`
  - Pod labels: `app=rise-riscv-runner`, `riseproject.com/org_id={org_id}`, `riseproject.com/board={k8s_pool}`
- **Remove** `"riseproject.com/job_id"` label from pods
- Remove `find_pod_by_job_id()` function (no longer needed)
- Add FIXME block about `activeDeadlineSeconds` for JIT runner timeout

### 6. Delete `container/runner.py`

Duplicate of `k8s.py`. Remove it.

### 7. `container/worker.py` — Single-thread loop

**Main loop** (~15s interval, wakes on `queue_event`):
```
while True:
    gh_reconcile()
    demand_match()
    cleanup_pods()
    wait(queue_event, 15s)
```

**`gh_reconcile()`**:
- For each active job in Redis, check via `GET /repos/{owner}/{repo}/actions/jobs/{job_id}`
- Group by installation_id to minimize auth calls
- If GH says completed but Redis disagrees → `complete_job()`
- If GH says in_progress but Redis says pending → `update_job_running()`

**`demand_match()`**:
- Get all pending jobs in FIFO order: `get_pending_jobs()` (from global `{env}:pending` ZSET)
- Track per-org worker counts (loaded once at start from Redis)
- For each pending job in FIFO order:
  1. Read job hash → get org_id, k8s_pool, k8s_image, installation_id, org_name, labels
  2. Check org has capacity: `(job_count, worker_count) = get_pool_demand(org_id, k8s_pool)`
     - If `job_count <= worker_count` → skip (demand already met for this pool)
     - If org's total workers across all pools >= `max_workers` → skip
  3. Check k8s has capacity: `has_available_slot({"riseproject.dev/board": k8s_pool})`
     - If no capacity → skip (try next job, maybe different pool has capacity)
  4. Provision:
     - Generate random `runner_name`
     - `authenticate_app(installation_id)` → token
     - `ensure_runner_group(org_name, token, RUNNER_GROUP_NAME)` → group_id
     - `create_jit_runner_config(token, group_id, labels, org_name, runner_name)` → jit_config
     - `provision_runner(jit_config, runner_name, k8s_image, k8s_pool, org_id)` → pod
     - `mark_provisioned(job_id, runner_name)` → ZREM from pending, status=running
     - `add_worker(org_id, k8s_pool, runner_name)` → SADD to pool:workers
     - Update local worker count tracker

**`cleanup_pods()`**:
- List all runner pods (label `app=rise-riscv-runner`)
- For pods in Succeeded/Failed phase: delete pod, `remove_worker(org_id, k8s_pool, pod_name)` using pod labels (`riseproject.com/org_id`, `riseproject.com/board`)
- Also: scan completed job hashes older than N minutes and `cleanup_job()` them

### 8. `container/Dockerfile`

Update COPY to add `github.py`, remove `runner.py`.

### 9. `README.md` — Architecture documentation

Add detailed sections:
- **Architecture overview**: Components (gh-app, Redis, k8s, GitHub), demand matching model
- **Sequence diagrams**: Queued webhook flow, provisioning flow, completed flow, cancellation flow
- **State machines**: Job lifecycle (pending→running→completed), Worker lifecycle (created→running→succeeded/failed)
- **Redis schema**: All keys, types, and relationships
- **Demand matching algorithm**: Formula, per-pool counting, max_workers cap
- **Configuration**: ORG_CONFIG fields and their meaning

### 10. Tests

- **`tests/conftest.py`**: Add `ORG_CONFIG`, `ALLOWED_ORGS`, `STAGING_ORGS`, `RUNNER_GROUP_NAME` to mock
- **`tests/test_handler.py`**: Remove GitHub API tests, add `in_progress` test, verify `db.store_job` includes k8s_pool/k8s_image
- **`tests/test_github.py`** (NEW): Tests for all GitHub API functions + `get_job_status`
- **`tests/test_worker.py`**: Test demand_match per-pool logic, max_workers cap, gh_reconcile, cleanup
- **`tests/test_db.py`** (rename from `test_redis.py`): Test pool-based operations, SCARD counting
- **`tests/test_k8s.py`**: Update `provision_runner` signature (k8s_pool instead of k8s_spec, org_id instead of job_id)
- **Delete `tests/test_runner.py`**

## Implementation Sequence

1. `constants.py`
2. `github.py` (new)
3. `db.py` (rewrite)
4. `handler.py` (simplify)
5. `k8s.py` + delete `runner.py`
6. `worker.py` (rewrite)
7. `Dockerfile`
8. Tests
9. `README.md`

## Verification

1. `pytest` — all tests pass
2. Deploy to staging:
   - `queued` → job in Redis, pool:jobs set updated
   - Worker → pod created, pool:workers set updated
   - `in_progress` → hash status updated
   - `completed` → job removed from pool:jobs, pod cleaned up, removed from pool:workers
3. Cancel a job → GH reconciliation detects and cleans up within ~15s
4. Verify per-pool counting: `redis-cli SCARD staging:pool:{org_id}:{k8s_pool}:jobs`
5. Test max_workers cap for luhenry org
6. Test mixed platforms in same org