import functools
import logging
import kubernetes as k8s
import yaml

from constants import *

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _init_client():
    """Create a Kubernetes API client from a kubeconfig env var."""
    return k8s.config.new_client_from_config_dict(yaml.safe_load(KUBECONFIG))


def provision_runner(jit_config, runner_name, k8s_image, k8s_pool, entity_id):
    """Provision a new runner in a Kubernetes pod.

    k8s_pool is the board name (e.g. "scw-em-rv1"). The nodeSelector is
    reconstructed internally from it.
    """
    node_selector = {"riseproject.dev/board": k8s_pool}

    with _init_client() as client:
        api = k8s.client.CoreV1Api(client)

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": runner_name,
                "labels": {
                    "app": "rise-riscv-runner",
                    "riseproject.com/entity_id": str(entity_id),
                    "riseproject.com/board": k8s_pool,
                },
            },
            "spec": {
                "nodeSelector": node_selector,
                # 24h queue limit + 5d execution limit + 2h buffer = 525600s
                "activeDeadlineSeconds": 525600,
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "runner",
                        "image": k8s_image,
                        "imagePullPolicy": "Always",
                        "command": ["/bin/bash", "-eux", "-o", "pipefail", "-c"],
                        "args": [
                            f"./run.sh --jitconfig {jit_config}"
                        ],
                        "env": [
                            {"name": "GITHUB_ACTIONS_RUNNER_TRACE", "value": "1"},
                            {"name": "DOCKER_HOST", "value": "tcp://localhost:2376"},
                            {"name": "DOCKER_TLS_CERTDIR", "value": "/docker-certs"},
                            {"name": "DOCKER_TLS_VERIFY", "value": "1"},
                            {"name": "DOCKER_CERT_PATH", "value": "/docker-certs/client"},
                        ],
                        "volumeMounts": [
                            {
                                "name": "docker-certs",
                                "mountPath": "/docker-certs",
                                "readOnly": True,
                            },
                            {
                                "name": "workspace",
                                "mountPath": "/home/runner/_work",
                            },
                        ],
                        "resources": {
                            "limits": {
                                "riseproject.com/runner": "1",
                            }
                        }
                    },
                ],
                "initContainers": [
                    {
                        # Docker-in-Docker sidecar for runner container to run DinD-enabled jobs
                        "name": "dind",
                        "image": RUNNER_IMAGE_DIND,
                        "imagePullPolicy": "Always",
                        "restartPolicy": "Always", # makes it a "sidecar"
                        "securityContext": {"privileged": True},
                        "args": [
                            # The DinD container's docker0 bridge defaults to MTU 1500, but the
                            # underlying Flannel/CNI overlay network only supports 1450, causing
                            # large packets (like TLS ClientHello) to be silently dropped and TLS
                            # handshakes to hang.
                            # Fix: set dockerd --mtu=1450 in the DinD container to match the pod
                            # network's path MTU.
                            "--mtu=1450",
                        ],
                        "env": [
                            {"name": "DOCKER_TLS_CERTDIR", "value": "/docker-certs"},
                        ],
                        "volumeMounts": [
                            {
                                "name": "docker-certs",
                                "mountPath": "/docker-certs",
                            },
                            {
                                "name": "docker-storage",
                                "mountPath": "/var/lib/docker",
                            },
                            {
                                "name": "workspace",
                                "mountPath": "/home/runner/_work",
                            },
                        ],
                    },
                ],
                "volumes": [
                    {
                        "name": "docker-certs",
                        "emptyDir": {},
                    },
                    {
                        "name": "docker-storage",
                        "emptyDir": {},
                    },
                    {
                        "name": "workspace",
                        "emptyDir": {},
                    },
                ],
            }
        }

        api.create_namespaced_pod(body=pod_manifest, namespace=KUBENAMESPACE)


def delete_pod(pod):
    """Delete a runner pod."""
    assert pod, "Pod must be provided to delete it"
    with _init_client() as client:
        api = k8s.client.CoreV1Api(client)
        try:
            api.delete_namespaced_pod(name=pod.metadata.name, namespace=KUBENAMESPACE)
            logger.info("Deleted runner pod %s", pod.metadata.name)
            return f"Pod {pod.metadata.name} deleted successfully."
        except k8s.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.debug("Pod %s not found, already deleted", pod.metadata.name)
                return f"Pod {pod.metadata.name} not found."
            raise


def has_available_slot(node_selector):
    """Check if there's an available runner slot on nodes matching the selector."""
    with _init_client() as client:
        api = k8s.client.CoreV1Api(client)

        nodes = api.list_node()
        matching_nodes = [
            node for node in nodes.items
            if all(node.metadata.labels.get(k) == v for k, v in node_selector.items())
        ]
        total = sum(
            int(node.status.allocatable.get("riseproject.com/runner", "0"))
            for node in matching_nodes
        )

        pods = api.list_namespaced_pod(
            namespace=KUBENAMESPACE, label_selector="app=rise-riscv-runner"
        )
        active = sum(
            1 for p in pods.items
            if p.status.phase in ("Pending", "Running")
            and p.spec.node_selector == node_selector
        )

        available = total - active
        logger.debug("Capacity check: node_selector=%s, total=%d, active=%d, available=%d",
                     node_selector, total, active, available)
        return available > 0


def get_pod_events(pod_name):
    """Get events for a specific pod, sorted by last timestamp."""
    with _init_client() as client:
        api = k8s.client.CoreV1Api(client)
        events = api.list_namespaced_event(
            namespace=KUBENAMESPACE,
            field_selector=f"involvedObject.name={pod_name}",
        )
        sorted_events = sorted(
            events.items,
            key=lambda e: e.last_timestamp or e.event_time or e.metadata.creation_timestamp,
        )
        return sorted_events


def list_pods():
    """Get all runner pods."""
    with _init_client() as client:
        api = k8s.client.CoreV1Api(client)
        pods = api.list_namespaced_pod(
            namespace=KUBENAMESPACE, label_selector="app=rise-riscv-runner"
        )
        return pods.items
