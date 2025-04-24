#!/usr/bin/env python

import time
import random
import json
import logging
import os
import asyncio 
from decimal import Decimal, InvalidOperation
import copy

from kubernetes_asyncio import client, config, watch

# --- Configuration ---
# Determine log level from environment variable, default to INFO
log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable for the API client, will be initialized in main
v1 = None
scheduler_name = os.getenv('SSI_SCHEDULER_NAME', 'custom-scheduler')
SCHEDULE_INTERVAL_SECONDS = float(os.getenv('SSI_SCHEDULE_INTERVAL_SECONDS', 0.2)) # Reduced interval
NODE_CACHE_TTL_SECONDS = int(os.getenv('SSI_NODE_CACHE_TTL_SECONDS', 60))
DEBOUNCE_DELAY_SECONDS = float(os.getenv('SSI_DEBOUNCE_DELAY_SECONDS', 0.1)) # Reduced from 0.5 (Currently unused)

# Lock to prevent concurrent scheduling cycles
scheduling_lock = asyncio.Lock()

# Debounce configuration
_debounce_task = None # Task handle for debouncing

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

# Function to parse resource quantities (re-added)
def parse_resource_quantity(quantity_str):
    """Parses Kubernetes resource quantity strings (CPU and Memory) into base units.

    Args:
        quantity_str (str): The resource string (e.g., '100m', '1', '2Gi', '512Mi').

    Returns:
        Decimal: The quantity in base units (millicpus for CPU, bytes for Memory), or Decimal('0') if parsing fails.
    """
    if not quantity_str:
        return Decimal('0')

    try:
        # Handle CPU (m for milli, or whole cores)
        if quantity_str.endswith('m'):
            # Support for millicpus like '100m'
            return Decimal(quantity_str[:-1])
        elif quantity_str.isdigit() or '.' in quantity_str:
            # Support for whole cores like '1', '0.5', convert to millicpus
            return Decimal(quantity_str) * 1000

        # Handle Memory (Ki, Mi, Gi, Ti, Pi, Ei - using 1024 base)
        # Simplified suffixes for common usage
        suffixes = {
            'Ki': 1024, 'K': 1000, # Kilo
            'Mi': 1024**2, 'M': 1000**2, # Mega
            'Gi': 1024**3, 'G': 1000**3, # Giga
            'Ti': 1024**4, 'T': 1000**4, # Tera
            'Pi': 1024**5, 'P': 1000**5, # Peta
            'Ei': 1024**6, 'E': 1000**6  # Exa
        }
        # Check for two-letter suffixes first (Ki, Mi, etc.)
        if len(quantity_str) > 2 and quantity_str[-2:] in suffixes:
            suffix = quantity_str[-2:]
            num_part = quantity_str[:-2]
            return Decimal(num_part) * suffixes[suffix]
        # Then check for one-letter suffixes (K, M, etc.)
        elif len(quantity_str) > 1 and quantity_str[-1] in suffixes:
            suffix = quantity_str[-1]
            num_part = quantity_str[:-1]
            return Decimal(num_part) * suffixes[suffix]
        # If no recognized suffix and not CPU-like, try direct decimal conversion (e.g., for bare byte values?)
        else:
            return Decimal(quantity_str) # Assuming bytes if no suffix
            
    except (InvalidOperation, ValueError, TypeError) as e:
        logging.warning(f"Could not parse resource quantity '{quantity_str}': {e}")
        return Decimal('0')

async def get_pending_pods(v1):
    """Fetches all pods in Pending state assigned to this scheduler."""
    if not v1:
        logging.error("get_pending_pods: API client not initialized.")
        return []
    pending_pods_list = []
    try:
        logging.debug(f"Fetching pending pods for scheduler '{scheduler_name}'...")
        # Fetch pods based on phase and scheduler name
        pods = await v1.list_pod_for_all_namespaces(
            field_selector='status.phase=Pending',
            # We still need to filter by schedulerName in the loop below
            _request_timeout=10
        )

        for pod in pods.items:
            # Filter based on scheduler name
            if pod.spec.scheduler_name == scheduler_name:
                 # Include all pending pods for this scheduler, regardless of nodeName status
                pending_pods_list.append(pod)

        logging.info(f"Found {len(pending_pods_list)} pending pods assigned to {scheduler_name}.")
        return pending_pods_list

    except asyncio.TimeoutError:
        logging.warning("get_pending_pods: Timeout fetching pods.")
        return []
    except client.ApiException as e:
        logging.error(f"get_pending_pods: API error fetching pods: {e}")
        return []
    except Exception as e:
        logging.error(f"get_pending_pods: Unexpected error: {e}", exc_info=True)
        return []

async def get_schedulable_nodes(v1): 
    """
    Fetches list of nodes, filters out unschedulable/control-plane nodes,
    and returns a list of dictionaries containing node name and allocatable resources.
    """
    available_nodes_info = [] # Changed from ready_nodes
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
                continue # Skip if unschedulable

            # Check taints for NoSchedule effect
            has_noschedule_taint = False
            if node.spec and node.spec.taints:
                for taint in node.spec.taints:
                    if taint.effect == 'NoSchedule':
                        is_schedulable = False
                        logging.debug(f"Node {node.metadata.name} has NoSchedule taint: {taint.key}={taint.value}")
                        break # Found a NoSchedule taint, no need to check others
                if not is_schedulable:
                    continue # Skip if NoSchedule taint

            # Check Ready condition
            if node.status and node.status.conditions:
                for condition in node.status.conditions:
                    if condition.type == 'Ready' and condition.status == 'True':
                        is_ready = True
                        break

            if not is_ready:
                logging.debug(f"Node {node.metadata.name} is not Ready, skipping.")
                continue

            # Extract allocatable resources (Added back)
            allocatable_cpu_m = Decimal('0')
            allocatable_mem_bytes = Decimal('0')
            if node.status and node.status.allocatable:
                allocatable_cpu_m = parse_resource_quantity(node.status.allocatable.get('cpu', '0'))
                allocatable_mem_bytes = parse_resource_quantity(node.status.allocatable.get('memory', '0'))

            node_info = {
                'name': node.metadata.name,
                'allocatable_cpu_m': allocatable_cpu_m,
                'allocatable_mem_bytes': allocatable_mem_bytes
            }
            available_nodes_info.append(node_info)
            logging.debug(f"Node {node.metadata.name} is available. Allocatable: CPU={allocatable_cpu_m}m, Mem={allocatable_mem_bytes}b")

    except client.ApiException as e:
        logging.error(f"Error listing or processing nodes: {e}")
    except Exception as e:
        logging.exception("Unexpected error processing nodes")
    
    logging.debug(f"Found {len(available_nodes_info)} available nodes for scheduling.")
    return {node['name']: node for node in available_nodes_info} # Return dict of dicts

async def schedule_pod(pod_name, node_name, namespace): 
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

async def select_highest_priority_pending_pods(v1):
    """Fetches pending pods, filters out image pull issues, groups by priority, and returns the highest priority group."""
    logging.debug("Fetching pending pods for scheduling cycle...")
    # Fetch ALL pods in the cluster initially
    try:
        all_pods = await v1.list_pod_for_all_namespaces(watch=False)
    except client.ApiException as e:
        logging.error(f"API Error fetching pods: {e}")
        return None, []
    except Exception as e:
        logging.error(f"Unexpected error fetching pods: {e}")
        return None, []

    runnable_pending_pods = []
    logging.debug(f"  Found {len(all_pods.items)} pods total. Filtering for Pending & {scheduler_name} & no image issues...")
    for pod in all_pods.items:
        if pod.spec.scheduler_name == scheduler_name and pod.status and pod.status.phase == 'Pending':
            # Now check for image pull issues
            is_image_pull_issue = False
            if pod.status.container_statuses:
                for status in pod.status.container_statuses:
                    if status.state and status.state.waiting and status.state.waiting.reason in ['ErrImagePull', 'ImagePullBackOff']:
                        logging.info(f"  Excluding pod {pod.metadata.namespace}/{pod.metadata.name} due to ImagePull issue ({status.state.waiting.reason}).")
                        is_image_pull_issue = True
                        break # Found an issue, no need to check other containers
                
            if not is_image_pull_issue:
                runnable_pending_pods.append(pod)
                logging.debug(f"    -> Included runnable pending pod: {pod.metadata.namespace}/{pod.metadata.name}")
 
    if not runnable_pending_pods:
        logging.info("No runnable pending pods found for scheduler.")
        return None, [] # No priority, empty list

    logging.info(f"Found {len(runnable_pending_pods)} runnable pending pods assigned to {scheduler_name}.")
    # Group Pods by Priority
    pods_by_priority = {}
    for pod in runnable_pending_pods:
        priority = get_pod_priority(pod)
        if priority not in pods_by_priority:
            pods_by_priority[priority] = []
        pods_by_priority[priority].append(pod)

    if not pods_by_priority:
        logging.info("No pending pods with priority found to schedule.")
        return None, []

    # Select Highest Priority Group (lowest numerical value)
    highest_priority = min(pods_by_priority.keys())
    pods_to_schedule_this_cycle = pods_by_priority[highest_priority]
    logging.info(f"Highest priority group to consider: {highest_priority} ({len(pods_to_schedule_this_cycle)} pods)")

    return highest_priority, pods_to_schedule_this_cycle


async def find_and_schedule_pod(pod_to_schedule, available_nodes, v1):
    """Processes a single pod: checks status, calculates resources, finds node, schedules, and handles potential deletion of old instance."""
    pod_name = pod_to_schedule.metadata.name
    pod_namespace = pod_to_schedule.metadata.namespace
    original_node_name = pod_to_schedule.spec.node_name # Store original node name if set
    should_attempt_reschedule = False
    priority = get_pod_priority(pod_to_schedule) # For logging

    # --- Check if Pod already has a nodeName and determine if rescheduling is needed --- 
    if original_node_name:
        logging.debug(f"Pod {pod_namespace}/{pod_name} has nodeName '{original_node_name}' set. Checking status...")
        pod_status = pod_to_schedule.status
        if pod_status and pod_status.phase == 'Failed':
            # Optionally check for specific reasons like Evicted for more targeted rescheduling
            if pod_status.reason == 'Evicted':
                 logging.warning(f"Pod {pod_namespace}/{pod_name} on node '{original_node_name}' is Failed with reason '{pod_status.reason}'. Attempting reschedule.")
                 should_attempt_reschedule = True
            else:
                 # You might want to handle other Failed reasons differently or ignore them
                 logging.warning(f"Pod {pod_namespace}/{pod_name} on node '{original_node_name}' is Failed but reason is '{pod_status.reason}' (not Evicted). Skipping reschedule.")
                 return False
        elif pod_status and pod_status.phase == 'Pending':
             # Check for image pull issues ONLY if it's Pending with nodeName
             is_image_pull_issue = False
             if pod_status.container_statuses:
                 for status in pod_status.container_statuses:
                    if status.state and status.state.waiting and status.state.waiting.reason in ['ErrImagePull', 'ImagePullBackOff']:
                        logging.info(f"Pod {pod_namespace}/{pod_name} on node '{original_node_name}' has ImagePull issue ({status.state.waiting.reason}). Skipping.")
                        is_image_pull_issue = True
                        break
             if is_image_pull_issue:
                  return False # Skip image pull issues
             else:
                  logging.debug(f"Pod {pod_namespace}/{pod_name} is still Pending on node '{original_node_name}'. Allowing Kubelet to manage. Skipping reschedule by custom scheduler.")
                  return False # Do not reschedule if just Pending with nodeName
        else:
            # Pod might be Running, Succeeded, or Unknown phase with a nodeName - leave it alone
            logging.debug(f"Pod {pod_namespace}/{pod_name} on node '{original_node_name}' has phase '{pod_status.phase if pod_status else 'Unknown'}'. Skipping reschedule.")
            return False
    else:
        # Pod has no nodeName, proceed with normal scheduling calculation
        logging.debug(f"Pod {pod_namespace}/{pod_name} has no nodeName. Proceeding with standard scheduling.")

    # --- Calculate Pod Resource Requests --- 
    requested_cpu_m = Decimal('0')
    requested_mem_bytes = Decimal('0')
    logging.debug(f"  Calculating requests for {pod_namespace}/{pod_name}:")
    if pod_to_schedule.spec and pod_to_schedule.spec.containers:
        for container in pod_to_schedule.spec.containers:
            container_cpu_req = Decimal('0')
            container_mem_req = Decimal('0')
            if container.resources and container.resources.requests:
                cpu_req_str = container.resources.requests.get('cpu', '0')
                mem_req_str = container.resources.requests.get('memory', '0')
                container_cpu_req = parse_resource_quantity(cpu_req_str)
                container_mem_req = parse_resource_quantity(mem_req_str)
                logging.debug(f"    Container '{container.name}': requests CPU='{cpu_req_str}', Mem='{mem_req_str}' -> Parsed: {container_cpu_req}m, {container_mem_req}b")
            else:
                logging.debug(f"    Container '{container.name}': No resource requests specified.")
            requested_cpu_m += container_cpu_req
            requested_mem_bytes += container_mem_req
    logging.info(f"  => Pod {pod_namespace}/{pod_name} TOTAL requests: CPU={requested_cpu_m}m, Mem={requested_mem_bytes}b")
    
    # --- Find Suitable Node --- 
    suitable_node_name = None
    # Create a list of nodes and shuffle it for basic load spreading among suitable nodes
    shuffled_nodes = list(available_nodes.items())
    random.shuffle(shuffled_nodes)
    
    logging.debug(f"Checking {len(shuffled_nodes)} nodes for pod {pod_namespace}/{pod_name}...")
    for node_name, node_status in shuffled_nodes:
        # Check if node has enough allocatable resources
        cpu_sufficient = node_status['allocatable_cpu_m'] >= requested_cpu_m
        mem_sufficient = node_status['allocatable_mem_bytes'] >= requested_mem_bytes
        
        if cpu_sufficient and mem_sufficient:
            logging.info(f"  -> Found suitable node for {pod_namespace}/{pod_name}: {node_name} (CPU: {node_status['allocatable_cpu_m']}m >= {requested_cpu_m}m, Mem: {node_status['allocatable_mem_bytes']}b >= {requested_mem_bytes}b)")
            suitable_node_name = node_name
            break # Found a node, stop searching
        else:
            logging.debug(f"  Node {node_name} insufficient for {pod_namespace}/{pod_name}. Needed CPU/Mem: {requested_cpu_m}m/{requested_mem_bytes}b, Available CPU/Mem: {node_status['allocatable_cpu_m']}m/{node_status['allocatable_mem_bytes']}b")

    # --- Schedule Pod and Handle Deletion --- 
    if suitable_node_name:
        # Attempt the binding
        await schedule_pod(pod_name, suitable_node_name, pod_namespace)
        
        # --- !! Delete Original Pod if Rescheduled !! ---
        if should_attempt_reschedule:
            logging.info(f"Rescheduling successful for {pod_namespace}/{pod_name} to node {suitable_node_name}. Attempting to delete original pod instance.")
            try:
                await v1.delete_namespaced_pod(
                    name=pod_name, 
                    namespace=pod_namespace, 
                    body=client.V1DeleteOptions(propagation_policy='Background') # 'Background' deletion is usually fine
                )
                logging.info(f"Deletion request sent successfully for original pod {pod_namespace}/{pod_name}.")
            except client.ApiException as e:
                # Log error but continue scheduling other pods
                logging.error(f"API Error deleting original pod {pod_namespace}/{pod_name}: {e}. Manual cleanup might be required.")
            except Exception as e:
                logging.error(f"Unexpected error deleting original pod {pod_namespace}/{pod_name}: {e}. Manual cleanup might be required.")
        # --- End Deletion Logic ---
        return True # Scheduling attempt was made

    else:
        logging.warning(f"  -> No suitable node found with sufficient resources for pod {pod_namespace}/{pod_name} (Prio: {priority}) in this cycle. Pod remains pending.")
        return False # No scheduling attempt made


async def attempt_scheduling_cycle(v1):
    """Orchestrates a single scheduling cycle: gets nodes, finds highest priority pods, and attempts to schedule them."""
    logging.info("--- Starting Scheduling Cycle ---")
    
    # Get current node status
    available_nodes = await get_schedulable_nodes(v1)
    if not available_nodes:
        logging.warning("No schedulable nodes found. Skipping cycle.")
        return False # No nodes, can't schedule

    # Select highest priority pods
    highest_priority, pods_to_schedule = await select_highest_priority_pending_pods(v1)

    if not pods_to_schedule:
        logging.info("No pending pods to schedule in this cycle.")
        return False # No pods to schedule

    logging.info(f"Attempting to schedule {len(pods_to_schedule)} pods with priority {highest_priority}")
    
    scheduled_a_pod_this_cycle = False
    # Iterate through the selected pods and try to schedule them one by one
    # Note: We are processing sequentially within the highest priority group for simplicity.
    # Concurrent binding could be reintroduced if performance is critical.
    for pod in pods_to_schedule:
        # Pass a copy of available_nodes. Modifications inside find_and_schedule_pod (if any) won't affect subsequent pods in this loop.
        nodes_copy = copy.deepcopy(available_nodes) 
        scheduled = await find_and_schedule_pod(pod, nodes_copy, v1)
        if scheduled:
            scheduled_a_pod_this_cycle = True
            # No optimistic update of the main 'available_nodes' map here.
            # Each pod scheduling attempt uses the node state known at the start of the cycle.
 
 
    if scheduled_a_pod_this_cycle:
        logging.info(f"--- Scheduling Cycle Complete: Attempted to schedule {len(pods_to_schedule)} pod(s) for priority {highest_priority}. ---")
    else:
        logging.info(f"--- Scheduling Cycle Complete: No pods could be scheduled for priority {highest_priority}. ---")

    return scheduled_a_pod_this_cycle # Return whether any scheduling attempt happened


async def check_if_pending_pods_exist(v1):
    """Quickly checks if any pods managed by this scheduler are in Pending state."""
    if not v1:
        logging.error("check_if_pending_pods_exist: API client not initialized.")
        return False
    try:
        logging.debug("Checking for any pending pods...")
        pending_pods = await v1.list_pod_for_all_namespaces(
            field_selector="status.phase=Pending",
            # We need to check schedulerName after fetching, as it's not a standard field selector
            # label_selector= can be used if you add labels to your pods
            _request_timeout=10 # Add a timeout
        )
        for pod in pending_pods.items:
            if pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
                logging.info("Pending pods found.")
                return True # Found at least one relevant pending pod
        logging.info("No relevant pending pods found.")
        return False
    except asyncio.TimeoutError:
        logging.warning("check_if_pending_pods_exist: Timeout checking for pending pods.")
        return False # Assume no pods on timeout to avoid unnecessary cycles
    except client.ApiException as e:
        logging.error(f"check_if_pending_pods_exist: API error checking pods: {e}")
        return False # Assume no pods on API error
    except Exception as e:
        logging.error(f"check_if_pending_pods_exist: Unexpected error: {e}", exc_info=True)
        return False

async def watch_pods():
    """Watches for Pod events and logs them. Does not trigger scheduling directly anymore."""
    global _debounce_task
    v1 = client.CoreV1Api()
    w = watch.Watch()
    logging.info("Starting to watch Pod events...")
    try:
        async for event in w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=0):
            pod = event['object']
            pod_name = pod.metadata.name
            pod_namespace = pod.metadata.namespace

            # Filter pods managed by this scheduler and in Pending state
            if pod.spec.scheduler_name == scheduler_name and pod.status.phase == 'Pending' and not pod.spec.node_name:
                logging.info(f"Detected relevant pod event: {event['type']} for pod {pod_namespace}/{pod_name}")
                # No longer setting event or using debounce timer for triggering
                # Triggering is now handled by the main loop's fixed interval

            elif pod.spec.scheduler_name == scheduler_name and event['type'] == 'DELETED':
                 logging.info(f"Detected relevant pod event: {event['type']} for pod {pod_namespace}/{pod_name}")
                 # Optionally, trigger scheduling if a deletion might free up resources
                 # For simplicity, we'll rely on the fixed interval for now.

    except asyncio.CancelledError:
        logging.info("Pod watcher task cancelled.")
    except Exception as e:
        logging.error(f"Error in pod watcher: {e}", exc_info=True)
        # Potentially add retry logic or re-initialization here
    finally:
        logging.info("Pod watcher stopped.")
        w.stop()

async def main():
    global v1 # Declare that we intend to modify the global v1 variable
    logging.info("Custom scheduler starting...")
    try:
        if os.getenv('KUBERNETES_SERVICE_HOST'):
            logging.info("Running inside a cluster, using service account credentials.")
            config.load_incluster_config()
        else:
            logging.info("Running outside a cluster, using kubeconfig.")
            config.load_kube_config()

        # Initialize the CoreV1Api client and assign it to the global v1
        v1 = client.CoreV1Api()

    except config.ConfigException as e:
        logging.error(f"Could not configure Kubernetes client: {e}")
        return

    logging.info("Kubernetes client configured successfully.")

    # Start the pod watcher task
    watcher_task = asyncio.create_task(watch_pods())

    logging.info("Entering main scheduling loop...")
    while True:
        try:
            # Check for pending pods before attempting to schedule
            are_pods_pending = await check_if_pending_pods_exist(v1)

            if are_pods_pending:
                logging.info("Attempting to acquire scheduling lock...")
                async with scheduling_lock:
                    logging.info("Lock acquired, attempting scheduling cycle.")
                    await attempt_scheduling_cycle(v1)
                    logging.info("Scheduling cycle finished.")
            else:
                logging.info("No pending pods detected, skipping scheduling cycle.")

            # Wait for SCHEDULE_INTERVAL_SECONDS before the next check/cycle
            logging.info(f"Waiting {SCHEDULE_INTERVAL_SECONDS} seconds before next check...")
            await asyncio.sleep(SCHEDULE_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logging.info("Main loop cancelled.")
            break
        except Exception as e:
            logging.error(f"Error in main scheduling loop: {e}", exc_info=True)
            # Avoid busy-looping on persistent errors
            await asyncio.sleep(5)

    # Clean up
    watcher_task.cancel()
    try:
        await watcher_task
    except asyncio.CancelledError:
        logging.info("Watcher task successfully cancelled.")

if __name__ == "__main__":
    # Initialize v1 to None globally initially
    v1 = None
    asyncio.run(main())