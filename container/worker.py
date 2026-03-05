import json
import logging
import threading

import redis_client
from runner import (
    authenticate_app,
    ensure_runner_group,
    create_jit_runner_config,
    provision_runner,
    delete_pod,
    has_available_slot,
    list_pods,
)

logger = logging.getLogger(__name__)

RUNNER_GROUP_NAME = "RISE RISC-V Runners"
POLL_INTERVAL = 5 # seconds between cleaning up tasks

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

        k8s_spec = json.loads(job.get("k8s_spec", "{}"))
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

            token = authenticate_app(payload)
            group_id = ensure_runner_group(payload, token, RUNNER_GROUP_NAME)
            jit_config, pod_name = create_jit_runner_config(
                payload, token, group_id, job_labels
            )
            provision_runner(payload, jit_config, pod_name, k8s_image, k8s_spec)

            with queue_lock:
                redis_client.finish_provisioning(r, job_id, pod_name)

        except Exception as e:
            logger.error("Failed to provision job %s: %s", job_id, e)
            with queue_lock:
                redis_client.requeue_job(r, job_id)


def cleanup_completed_jobs(r):
    """Delete pods for jobs that have been marked completed."""
    with queue_lock:
        completed = redis_client.get_completed_jobs_with_pods(r)

    for job_id, pod_name in completed:
        try:
            delete_pod(pod_name)
        except Exception as e:
            logger.error("Failed to delete pod %s for job %s: %s", pod_name, job_id, e)
            continue

        with queue_lock:
            redis_client.cleanup_job(r, job_id, pod_name)


def reconcile_orphan_pods(r):
    """Detect and clean up pods not tracked in Redis."""
    pods = list_pods()
    tracked_pods = redis_client.get_active_pods(r)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_annotations = pod.metadata.annotations or {}
        logger.debug("Checking pod %s for orphan status", pod_name)
        if pod_name not in tracked_pods:
            logger.debug("Found orphan pod %s not tracked in Redis", pod_name)
            # Check if pod is completed/failed — only clean up finished orphans
            if pod.status.phase in ("Succeeded", "Failed"):
                logger.warning("Cleaning up orphan pod %s (phase=%s)", pod_name, pod.status.phase)
                try:
                    delete_pod(pod_name)
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

                job_id = pod_annotations.get("riseproject.com/job_id")
                if not job_id:
                    logger.error("Pod %s missing riseproject.com/job_id annotation, cannot mark completed in Redis", pod_name)
                    continue
                # First mark completed in Redis
                with queue_lock:
                    redis_client.complete_job(r, job_id)
                # Then delete the pod
                try:
                    delete_pod(pod_name)
                except Exception as e:
                    logger.error("Failed to delete pod %s during reconciliation: %s", pod_name, e)


def dump_state_to_log(r):
    """Log the current state of the queue and active runners."""
    pending = redis_client.get_pending_jobs(r)
    active_pods = redis_client.get_active_pods(r)
    completed = redis_client.get_completed_jobs_with_pods(r)
    logger.info("Queue state: pending=%d, active_pods=%d, completed_with_pods=%d",
                len(pending), len(active_pods), len(completed))
    for job_id in pending:
        job = redis_client.get_job(r, job_id)
        logger.info("  pending job %s: status=%s", job_id, job.get("status") if job else "missing")
    for pod_name in active_pods:
        logger.info("  active pod: %s", pod_name)
    for job_id, pod_name in completed:
        logger.info("  completed job %s: pod=%s", job_id, pod_name)


def worker_loop(r):
    """Main worker loop — polls Redis and manages runner lifecycle."""
    while True:
        try:
            provision_pending_jobs(r)
            cleanup_completed_jobs(r)
            reconcile_orphan_pods(r)
            dump_state_to_log(r)
        except Exception as e:
            logger.error("Worker error: %s", e)

        with queue_event:
            queue_event.wait(timeout=POLL_INTERVAL)


def start_worker():
    """Start the background worker thread."""
    r = redis_client.connect()
    thread = threading.Thread(target=worker_loop, args=(r,), daemon=True)
    thread.start()
    logger.info("Background worker started")
    return thread
