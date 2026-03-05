import json
import logging
import threading

import redis_client
from runner import (
    authenticate_app,
    ensure_runner_group_on_org,
    create_jit_runner_config_on_org,
    provision_runner,
    delete_pod,
    has_available_slot,
    list_pods,
    find_pod_by_job_id,
)

logger = logging.getLogger(__name__)

RUNNER_GROUP_NAME = "RISE RISC-V Runners"
POLL_INTERVAL = 15 # seconds between cleaning up tasks

# Lock for atomic Redis state transitions between webhook handler and worker.
queue_lock = threading.Lock()
# Condition using queue_lock to wake the worker immediately when a job is enqueued.
queue_event = threading.Condition(lock=queue_lock)


def provision_pending_jobs(r):
    """Try to provision pending jobs that have matching cluster capacity."""
    pending = redis_client.get_pending_jobs(r)
    for job_id in pending:
        job = redis_client.get_job(r, job_id)
        if not job:
            continue

        k8s_spec = json.loads(job["k8s_spec"])
        node_selector = k8s_spec.get("nodeSelector", {})

        if not has_available_slot(node_selector):
            continue

        with queue_lock:
            if not redis_client.pick_job(r, job_id):
                continue  # job was cancelled between check and pickup

        # Provisioning happens outside the lock (slow GitHub/K8s API calls)
        try:
            payload = json.loads(job["payload"])
            k8s_image = job["k8s_image"]
            job_labels = json.loads(job.get("job_labels", "[]"))

            org_login = payload["repository"]["owner"]["login"]
            assert org_login, "Organization login must be provided in payload"

            installation_id = payload["installation"]["id"]
            assert installation_id, "Installation ID must be provided in payload"

            repo_id = payload["repository"]["id"]
            assert repo_id, "Repository ID must be provided in payload"

            repo_name = payload["repository"]["full_name"]
            assert repo_name, "Repository full name must be provided in payload"

            job_id = payload["workflow_job"]["id"]
            assert job_id, "Workflow job ID must be provided in payload"

            runner_name = f"rise-riscv-runner-{job_id}"

            installation_token = authenticate_app(installation_id, repo_id)
            runner_group_id = ensure_runner_group_on_org(org_login, installation_token, RUNNER_GROUP_NAME)
            jit_config = create_jit_runner_config_on_org(
                installation_token, runner_group_id, job_labels, org_login, runner_name)
            provision_runner(jit_config, runner_name, k8s_image, k8s_spec, job_id)

            with queue_lock:
                redis_client.finish_provisioning(r, job_id, runner_name)

            logger.info("Provisioned %s for org=%s, image=%s", runner_name, org_login, k8s_image)

        except Exception as e:
            logger.error("Failed to provision job %s: %s", job_id, e)
            with queue_lock:
                redis_client.requeue_job(r, job_id)


def cleanup_completed_jobs(r):
    """Delete pods for jobs that have been marked completed."""
    with queue_lock:
        completed = redis_client.get_completed_jobs_with_pods(r)

    for job_id in completed:
        pod = find_pod_by_job_id(job_id)
        if pod:
            try:
                delete_pod(pod)
            except Exception as e:
                logger.error("Failed to delete pod %s for job %s: %s", pod.metadata.name, job_id, e)
                continue
        else:
            logger.debug("No pod found for completed job %s", job_id)

        with queue_lock:
            redis_client.cleanup_job(r, job_id)


def reconcile_orphan_pods(r):
    """Detect and clean up pods not tracked in Redis."""
    pods = list_pods()
    tracked_job_ids = redis_client.get_active_jobs(r)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_labels = pod.metadata.labels or {}
        pod_job_id = pod_labels.get("riseproject.com/job_id")
        if not pod_job_id:
            logger.debug("Pod %s missing riseproject.com/job_id label, skipping", pod_name)
            continue

        logger.debug("Checking pod %s for orphan status", pod_name)
        if pod_job_id not in tracked_job_ids:
            logger.debug("Found orphan pod %s not tracked in Redis", pod_name)
            # Check if pod is completed/failed — only clean up finished orphans
            if pod.status.phase in ("Succeeded", "Failed"):
                logger.warning("Cleaning up orphan pod %s (phase=%s)", pod_name, pod.status.phase)
                try:
                    delete_pod(pod)
                except Exception as e:
                    logger.error("Failed to clean up orphan pod %s: %s", pod_name, e)
            elif pod.status.phase in ("Unknown"):
                logger.warning("Pod %s in Unknown phase, may require manual investigation", pod_name)
            else:
                logger.warning("Pod %s in unexpected phase %s, skipping automatic cleanup", pod_name, pod.status.phase)
        else:
            logger.debug("Pod %s is tracked in Redis, checking if completed on k8s anyway", pod_name)
            # Double-check if pod is completed on k8s but not marked completed in Redis, to handle missed webhooks
            if pod.status.phase in ("Succeeded", "Failed"):
                logger.warning("Pod %s is tracked in Redis but in %s phase, marking completed in Redis", pod_name, pod.status.phase)

                job_id = pod_labels.get("riseproject.com/job_id")
                if not job_id:
                    logger.error("Pod %s missing riseproject.com/job_id label, cannot mark completed in Redis", pod_name)
                    continue
                # First mark completed in Redis
                with queue_lock:
                    redis_client.complete_job(r, job_id)
                # Then delete the pod
                try:
                    delete_pod(pod)
                except Exception as e:
                    logger.error("Failed to delete pod %s during reconciliation: %s", pod_name, e)


def dump_state_to_log(r):
    """Log the current state of the queue and active runners."""
    pending = redis_client.get_pending_jobs(r)
    active = redis_client.get_active_jobs(r)
    completed = redis_client.get_completed_jobs_with_pods(r)
    logger.info("Queue state: pending=%d, active=%d, completed=%d",
                len(pending), len(active), len(completed))
    for job_id in pending:
        job = redis_client.get_job(r, job_id)
        logger.info("  pending job %s: status=%s", job_id, job.get("status") if job else "missing")
    for job_id in active:
        pod = find_pod_by_job_id(job_id)
        logger.info("  active job %s: (pod=%s, phase=%s)", job_id, pod.metadata.name if pod else "<not found>", pod.status.phase if pod else "<not found>")
    for job_id in completed:
        pod = find_pod_by_job_id(job_id)
        logger.info("  completed job %s: (pod=%s, phase=%s)", job_id, pod.metadata.name if pod else "<not found>", pod.status.phase if pod else "<not found>")


def worker_loop(r):
    """Main worker loop — polls Redis and manages runner lifecycle."""
    while True:
        try:
            provision_pending_jobs(r)
            cleanup_completed_jobs(r)
            reconcile_orphan_pods(r)
            dump_state_to_log(r)
        except Exception as e:
            logger.error("Worker error: %s\n%s", e, e.format_exc())

        with queue_event:
            queue_event.wait(timeout=POLL_INTERVAL)


def start_worker():
    """Start the background worker thread."""
    r = redis_client.connect()
    thread = threading.Thread(target=worker_loop, args=(r,), daemon=True)
    thread.start()
    logger.info("Background worker started")
    return thread
