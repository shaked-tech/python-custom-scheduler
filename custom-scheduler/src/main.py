#!/usr/bin/env python

import time
import random
import json
import logging
import os
import asyncio 

from kubernetes_asyncio import client, config, watch

# --- Configuration ---
# Determine log level from environment variable, default to INFO
log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable for the API client, will be initialized in main
v1 = None
scheduler_name = "custom-scheduler"

# Lock to prevent concurrent scheduling cycles
scheduling_lock = asyncio.Lock()
# Event to signal that scheduling might be needed
schedule_needed_event = asyncio.Event()

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

async def nodes_available(): 
    """
    Fetches list of nodes, filters out unschedulable ones and control plane nodes.
    Returns a list of available node names.
    """
    global v1
    ready_nodes = []
    try:
        node_list = await v1.list_node()
        for node in node_list.items:
            # Check if node has the control-plane label
            labels = node.metadata.labels or {}
            is_control_plane = 'node-role.kubernetes.io/control-plane' in labels
            if is_control_plane:
                logging.debug(f"Node {node.metadata.name} is a control plane node, skipping.")
                continue

            # Check node conditions for readiness and scheduling status
            is_ready = False
            is_schedulable = True # Assume schedulable unless unschedulable taint found or Spec.unschedulable=True

            if node.spec and node.spec.unschedulable:
                is_schedulable = False
                logging.debug(f"Node {node.metadata.name} is marked unschedulable (spec.unschedulable=True).")

            # Check taints for NoSchedule effect
            if is_schedulable and node.spec and node.spec.taints:
                for taint in node.spec.taints:
                    if taint.effect == 'NoSchedule':
                        is_schedulable = False
                        logging.debug(f"Node {node.metadata.name} has NoSchedule taint: {taint.key}={taint.value}")
                        break # Found a NoSchedule taint, no need to check others

            # Check Ready condition
            if node.status and node.status.conditions:
                for condition in node.status.conditions:
                    if condition.type == 'Ready' and condition.status == 'True':
                        is_ready = True
                        break

            if is_ready and is_schedulable:
                ready_nodes.append(node.metadata.name)
                logging.debug(f"Node {node.metadata.name} is Ready and Schedulable.")
            else:
                logging.debug(f"Node {node.metadata.name} is not suitable (Ready: {is_ready}, Schedulable: {is_schedulable}).")

    except client.ApiException as e:
        logging.error(f"Error listing or processing nodes: {e}")
    except Exception as e:
        logging.exception("Unexpected error processing nodes")
    
    logging.debug(f"Available nodes for scheduling: {ready_nodes}")
    return ready_nodes

async def schedule_pod(pod_name, node_name, namespace): 
    """Creates a binding for the pod to the node."""
    global v1
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
        await v1.create_namespaced_binding(namespace=namespace, body=body, _preload_content=False)
        logging.info(f"Successfully bound pod '{namespace}/{pod_name}' to node '{node_name}'")
        return True
    except client.ApiException as e:
        error_body = {}
        try:
            error_body = json.loads(e.body.decode('utf-8') if isinstance(e.body, bytes) else e.body)
        except (json.JSONDecodeError, AttributeError):
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

async def attempt_scheduling_cycle():
    """
    Fetches pending pods, sorts by priority, and attempts to schedule multiple pods concurrently,
    up to the number of available nodes.
    Returns True if schedulable pods were found, False otherwise.
    """
    global v1
    logging.debug("Attempting scheduling cycle.")
    available_nodes = await nodes_available()
    if not available_nodes:
        logging.warning("No nodes available for scheduling.")
        return False

    try:
        pod_list = await v1.list_pod_for_all_namespaces(field_selector="status.phase=Pending")
    except client.ApiException as e:
        logging.error(f"Error listing pods: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error listing pods: {e}")
        return False

    schedulable_pods = []
    for pod in pod_list.items:
        if pod.spec and pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
            schedulable_pods.append(pod)

    if not schedulable_pods:
        logging.debug("No pending pods found for this scheduler.")
        return False

    schedulable_pods.sort(key=lambda p: (get_pod_priority(p), -p.metadata.creation_timestamp.timestamp()), reverse=True)

    logging.info(f"Found {len(schedulable_pods)} pending pods and {len(available_nodes)} available nodes.")

    # --- Concurrent Scheduling Logic --- 
    tasks_to_run = []
    nodes_assigned_this_cycle = set()
    # Shuffle nodes to distribute pods somewhat randomly if priorities are equal
    random.shuffle(available_nodes)
    node_iter = iter(available_nodes)

    for pod_to_schedule in schedulable_pods:
        if len(nodes_assigned_this_cycle) >= len(available_nodes):
            logging.debug("All available nodes assigned in this cycle.")
            break # Stop if we've assigned all available nodes

        selected_node = next(node_iter, None)
        if selected_node:
            priority = get_pod_priority(pod_to_schedule)
            pod_name = pod_to_schedule.metadata.name
            pod_namespace = pod_to_schedule.metadata.namespace
            logging.info(f"  -> Preparing to schedule {pod_namespace}/{pod_name} (Prio: {priority}) to node {selected_node}")
            # Add task to schedule this pod
            tasks_to_run.append(schedule_pod(pod_name, selected_node, pod_namespace))
            nodes_assigned_this_cycle.add(selected_node)
        else:
            # Should not happen if len(nodes_assigned_this_cycle) < len(available_nodes)
            logging.warning("Ran out of nodes unexpectedly.")
            break

    if tasks_to_run:
        logging.info(f"Attempting to concurrently schedule {len(tasks_to_run)} pods...")
        # Run scheduling tasks concurrently
        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        # Log results (optional: check for exceptions/failures in results)
        successful_schedules = 0
        for i, result in enumerate(results):
            # Basic check if result is True (success from schedule_pod)
            if result is True:
                successful_schedules += 1
            elif isinstance(result, Exception):
                logging.error(f"  -> Error in scheduling task {i}: {result}")
            else:
                 # schedule_pod returned False or something unexpected
                 logging.warning(f"  -> Scheduling task {i} did not complete successfully (result: {result})")
        logging.info(f"Concurrent scheduling finished. Successfully scheduled {successful_schedules}/{len(tasks_to_run)} pods prepared in this cycle.")
    else:
        logging.debug("No pods were prepared for scheduling in this cycle.")

    return True # Schedulable pods were found (even if scheduling failed for some)

async def trigger_scheduling_cycle(): # New function to handle locking
    """Acquires the lock and runs the scheduling cycle. Prevents concurrent runs.
    Returns the result of attempt_scheduling_cycle (True if pods were found, False otherwise).
    """
    if scheduling_lock.locked():
        logging.debug("Scheduling cycle already in progress, skipping trigger.")
        return False # Indicate no new cycle was started

    async with scheduling_lock:
        logging.debug("Acquired scheduling lock.")
        result = False
        try:
            result = await attempt_scheduling_cycle()
        except Exception as e:
            logging.exception("Error during attempt_scheduling_cycle")
            result = False # Indicate potential failure/no success
        finally:
            logging.debug("Released scheduling lock.")
        return result

async def watch_pods(): # New function for dedicated watching
    """Watches for pending pods and sets the schedule_needed_event."""
    global v1
    w = watch.Watch()
    logging.info("Starting pod watcher...")
    while True:
        try:
            async for event in w.stream(v1.list_pod_for_all_namespaces):
                event_type = event['type']
                pod = event['object']

                if not hasattr(pod, 'spec') or not hasattr(pod, 'status') or not hasattr(pod, 'metadata'):
                    logging.debug(f"Watcher: Received non-pod event or incomplete pod object: {event_type}")
                    continue

                logging.debug(f"Watcher: Received event: {event_type} for pod {pod.metadata.namespace}/{pod.metadata.name}, phase: {pod.status.phase}, scheduler: {pod.spec.scheduler_name}")

                # If a relevant pod is pending, signal the scheduler loop
                if pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
                    logging.info(f"Watcher: Pending pod {pod.metadata.namespace}/{pod.metadata.name} detected. Signaling scheduler.")
                    schedule_needed_event.set() # Set the event

        except client.ApiException as e:
            if e.status == 410: # Gone
                logging.warning("Watcher: Stream closed (410 Gone), restarting...")
            else:
                logging.error(f"Watcher: API Error in stream: {e}. Restarting watch...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logging.info("Watcher: Task cancelled.")
            break
        except Exception as e:
            logging.exception(f"Watcher: Unexpected error in stream: {e}. Restarting watch...")
            await asyncio.sleep(5)
    logging.info("Pod watcher finished.")

async def run_scheduler_loop(): # New function for dedicated scheduling
    """Runs the scheduling cycle whenever signaled by the event."""
    logging.info("Starting scheduler loop...")
    while True:
        try:
            await schedule_needed_event.wait() # Wait until the event is set
            schedule_needed_event.clear() # Clear the event immediately
            logging.info("Scheduler loop: Event received, entering active scheduling phase.")
            while True: # Inner loop: keep running cycles as long as work is done
                logging.debug("Scheduler loop: Running a scheduling cycle attempt.")
                # Run the existing trigger function which handles locking and the cycle
                work_done = await trigger_scheduling_cycle()
                if not work_done:
                    logging.debug("Scheduler loop: No schedulable pods found in last cycle. Pausing.")
                    break # Exit inner loop if no work was done
                else:
                    logging.debug("Scheduler loop: Work potentially done in last cycle. Re-checking immediately.")
                    await asyncio.sleep(0.1) # Small delay before next check in inner loop

            logging.info("Scheduler loop: Paused, waiting for next event.")
        except asyncio.CancelledError:
            logging.info("Scheduler loop: Task cancelled.")
            break
        except Exception as e:
            # Catch errors within the scheduling loop itself
            logging.exception(f"Scheduler loop: Error during scheduling cycle execution: {e}")
            # Add a delay before trying again to avoid overwhelming logs/retries
            await asyncio.sleep(5)
    logging.info("Scheduler loop finished.")


# --- Main Entry Point ---
async def main():
    global v1
    logging.info(f"Initializing Kubernetes client...")
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
    except config.ConfigException as e:
        logging.error(f"Could not configure Kubernetes client: {e}")
        return
    except Exception as e:
        logging.error(f"Unexpected error during client initialization: {e}")
        return

    logging.info(f"Starting custom scheduler '{scheduler_name}'...")

    # Create tasks for the watcher and the scheduler loop
    watcher_task = asyncio.create_task(watch_pods())
    scheduler_task = asyncio.create_task(run_scheduler_loop())

    # Wait for tasks to complete (e.g., on cancellation)
    # Use gather to wait for both and handle potential exceptions
    done, pending = await asyncio.wait(
        [watcher_task, scheduler_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # If one task finishes (e.g., due to unhandled error), cancel the other
    for task in pending:
        task.cancel()

    # Wait for pending tasks to finish cancellation
    await asyncio.gather(*pending, return_exceptions=True)

    logging.info("Scheduler tasks finished.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user.")
    except Exception as e:
        logging.exception(f"An unexpected error occurred at the top level: {e}")
    finally:
        logging.info("Scheduler shutdown complete.")