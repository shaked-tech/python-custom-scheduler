#!/usr/bin/env python

import time
import random
import json
import logging
import os

from kubernetes import client, config, watch

# --- Configuration ---
# Determine log level from environment variable, default to INFO
log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# config.load_kube_config() # Use kubeconfig when running locally
config.load_incluster_config() # Use service account token when running in cluster

v1 = client.CoreV1Api()
scheduler_name = "custom-scheduler"
default_priority = 0

# --- Helper Functions ---

def get_pod_priority(pod):
    """Extracts priority from pod annotations. Defaults to -1 if missing or invalid."""
    try:
        annotations = pod.metadata.annotations or {}
        priority_str = annotations.get('priority') # Get value or None

        if priority_str is None:
            # Annotation is missing, assign lowest priority
            return -1
        else:
            # Annotation exists, try to convert to integer
            return int(priority_str)

    except (ValueError, TypeError):
        # Annotation exists but is not a valid integer
        logging.warning(f"Pod {pod.metadata.namespace}/{pod.metadata.name}: Invalid priority annotation value '{priority_str}'. Assigning lowest priority (-1).")
        return -1

def nodes_available():
    ready_nodes = []
    try:
        for n in v1.list_node().items:
            for status in n.status.conditions:
                if status.type == "Ready" and status.status == "True":
                    if n.spec.unschedulable is not True:
                        ready_nodes.append(n.metadata.name)
                    break
    except client.rest.ApiException as e:
        logging.error(f"Error listing nodes: {e}")
    return ready_nodes

def schedule_pod(pod_name, node_name, namespace):
    """Creates a binding for the pod to the node."""
    target = client.V1ObjectReference(
        kind="Node",
        api_version="v1",
        name=node_name
    )
    meta = client.V1ObjectMeta(
        name=pod_name,
    )
    body = client.V1Binding(
        target=target,
        metadata=meta
    )
    try:
        logging.info(f"Attempting to bind pod '{namespace}/{pod_name}' to node '{node_name}'")
        v1.create_namespaced_binding(namespace=namespace, body=body, _preload_content=False)
        logging.info(f"Successfully bound pod '{namespace}/{pod_name}' to node '{node_name}'")
        return True
    except client.rest.ApiException as e:
        error_body = {}
        try:
            error_body = json.loads(e.body)
        except json.JSONDecodeError:
            pass

        if e.status == 409:
            logging.warning(f"Conflict scheduling pod '{namespace}/{pod_name}' (status: {e.status}): {error_body.get('message', e.reason)}. It might already be scheduled.")
        elif e.status == 404:
            logging.warning(f"Pod '{namespace}/{pod_name}' not found (status: {e.status}): {error_body.get('message', e.reason)}. Skipping scheduling.")
        else:
            logging.error(f"API Error scheduling pod '{namespace}/{pod_name}' (status: {e.status}): {error_body.get('message', e.reason)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error scheduling pod '{namespace}/{pod_name}': {e}")
        return False


def attempt_scheduling_cycle():
    logging.debug("Starting scheduling cycle.")
    available_nodes = nodes_available()
    if not available_nodes:
        logging.warning("No nodes available for scheduling.")
        return

    try:
        pod_list = v1.list_pod_for_all_namespaces(field_selector="status.phase=Pending")
    except client.rest.ApiException as e:
        logging.error(f"Error listing pods: {e}")
        return

    schedulable_pods = []
    for pod in pod_list.items:
        if pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
            priority = get_pod_priority(pod)
            schedulable_pods.append((priority, pod))

    if not schedulable_pods:
        logging.debug("No pending pods found for this scheduler.")
        return

    schedulable_pods.sort(key=lambda x: (x[0], -x[1].metadata.creation_timestamp.timestamp()), reverse=True)

    logging.info(f"Found {len(schedulable_pods)} pending pods for scheduling across all namespaces. Highest priority: {schedulable_pods[0][0]}")
    logging.debug(f"Pending pods sorted by priority: {[f'{p[1].metadata.namespace}/{p[1].metadata.name}' for p in schedulable_pods]}")

    priority_to_schedule, pod_to_schedule = schedulable_pods[0]
    pod_name = pod_to_schedule.metadata.name
    pod_namespace = pod_to_schedule.metadata.namespace

    selected_node = random.choice(available_nodes)
    logging.info(f"Selected node '{selected_node}' for pod '{pod_namespace}/{pod_name}' (priority {priority_to_schedule})")

    schedule_pod(pod_name, selected_node, pod_namespace)


# --- Main Loop ---
def main():
    logging.info(f"Starting custom scheduler '{scheduler_name}' watching all namespaces...")
    w = watch.Watch()
    stream = w.stream(v1.list_pod_for_all_namespaces)

    for event in stream:
        event_type = event['type']
        pod = event['object']

        if not hasattr(pod, 'spec') or not hasattr(pod, 'status'):
            logging.debug(f"Received non-pod event or incomplete pod object: {event_type}")
            continue

        logging.debug(f"Received event: {event_type} for pod {pod.metadata.namespace}/{pod.metadata.name}, phase: {pod.status.phase}, scheduler: {pod.spec.scheduler_name}")

        if pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
            logging.info(f"Pending pod detected: {pod.metadata.namespace}/{pod.metadata.name}. Triggering scheduling cycle.")
            attempt_scheduling_cycle()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped.")
    except Exception as e:
        logging.exception(f"An unexpected error occurred in main loop: {e}")