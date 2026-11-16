
import json
import uuid
import time
import threading
import queue

from pathlib import Path

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException


BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "clusters.json"


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

def load_configuration():
    with open(CONFIG_FILE, "r") as file:
        return json.load(file)


def connect_cluster(cluster):
    kubeconfig_path = BASE_DIR / cluster["kubeconfig"]

    if not kubeconfig_path.is_file():
        raise FileNotFoundError(
            f"Kubeconfig not found: {kubeconfig_path}"
        )

    return config.new_client_from_config(
        config_file=str(kubeconfig_path)
    )


# --------------------------------------------------
# ORGANIZATION / CLUSTER SELECTION
# --------------------------------------------------

def select_organization(configuration):
    organizations = configuration["organizations"]
    org_ids = list(organizations.keys())

    print("\n========== SELECT ORGANIZATION ==========")

    for index, org_id in enumerate(org_ids, 1):
        print(f"{index}. {org_id}")

    try:
        choice = int(input("\nSelect organization: "))
    except ValueError:
        print("Enter a valid number.")
        return None, None

    if choice < 1 or choice > len(org_ids):
        print("Invalid selection.")
        return None, None

    org_id = org_ids[choice - 1]

    return org_id, organizations[org_id]


def select_cluster(organization):
    clusters = organization["clusters"]
    cluster_ids = list(clusters.keys())

    print("\n========== SELECT CLUSTER ==========")

    for index, cluster_id in enumerate(cluster_ids, 1):
        print(f"{index}. {cluster_id}")

    try:
        choice = int(input("\nSelect cluster: "))
    except ValueError:
        print("Enter a valid number.")
        return None, None

    if choice < 1 or choice > len(cluster_ids):
        print("Invalid selection.")
        return None, None

    cluster_id = cluster_ids[choice - 1]

    return cluster_id, clusters[cluster_id]


# --------------------------------------------------
# JOB STATUS HELPERS
# --------------------------------------------------

def get_job_state(job):
    conditions = job.status.conditions or []

    for condition in conditions:
        if condition.status == "True":

            if condition.type == "Complete":
                return "COMPLETED"

            if condition.type == "Failed":
                return "FAILED"

            if condition.type == "FailureTarget":
                return "FAILING"

    if job.status.active:
        return "RUNNING"

    return "PENDING"


def get_pod_state(pod):
    if not pod.status:
        return "UNKNOWN"

    phase = pod.status.phase or "UNKNOWN"

    # Include useful container-level information.
    container_statuses = pod.status.container_statuses or []

    for container in container_statuses:
        if container.state and container.state.waiting:
            reason = container.state.waiting.reason
            if reason:
                return f"{phase} ({reason})"

        if container.state and container.state.terminated:
            reason = container.state.terminated.reason
            if reason:
                return f"{phase} ({reason})"

    return phase


# --------------------------------------------------
# CREATE AND EXECUTE JOB
# --------------------------------------------------

def create_job(api_client, cluster, org_id):

    namespace = cluster["namespace"]
    job_name = "job-" + uuid.uuid4().hex[:8]

    labels = {
        "app": "job-orchestrator",
        "organization": org_id
    }

    container = client.V1Container(
        name="job-container",
        image="busybox:latest",
        command=["sh", "-c", "echo 'Job Starts!' && sleep 10 && echo 'Job step1.' && sleep 3 && echo 'Job step2. Job COmpleted'"]
    )

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        service_account_name=cluster.get(
            "runtime_service_account", "default"
        ),
        containers=[container]
    )

    pod_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(
            labels=labels
        ),
        spec=pod_spec
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            labels=labels
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            active_deadline_seconds=3600,
            template=pod_template
        )
    )

    batch_api = client.BatchV1Api(api_client)

    try:
        response = batch_api.create_namespaced_job(
            namespace=namespace,
            body=job
        )

        print("\n========== JOB CREATED ==========")
        print("Job ID:", response.metadata.name)
        print("Organization:", org_id)
        print("Namespace:", namespace)
        print("Image:", "busybox:latest")
        print("Status:", get_job_state(response))

        return response.metadata.name

    except ApiException as error:
        print("\nOpenShift API Error:")
        print(error.status, error.reason)

    return None


# --------------------------------------------------
# WATCH WORKER
# --------------------------------------------------

def watch_resource(
    kubeconfig_path,
    namespace,
    resource_type,
    resource_name,
    event_queue,
    stop_event
):
    """
    Maintains a Watch stream for either:
    - A specific Job
    - Pods belonging to a specific Job

    Reconnects after timeouts and relists after
    expired resource versions.
    """

    api_client = None

    try:
        api_client = config.new_client_from_config(
            config_file=str(kubeconfig_path)
        )

        if resource_type == "JOB":
            api = client.BatchV1Api(api_client)
            list_function = api.list_namespaced_job

            filters = {
                "field_selector": f"metadata.name={resource_name}"
            }

        else:
            api = client.CoreV1Api(api_client)
            list_function = api.list_namespaced_pod

            filters = {
                "label_selector": f"job-name={resource_name}"
            }

        while not stop_event.is_set():

            try:
                # Initial LIST: obtain the current state
                # and resource version.
                initial_data = list_function(
                    namespace=namespace,
                    **filters
                )

                resource_version = (
                    initial_data.metadata.resource_version
                )

                # Send initial state to the main thread.
                for item in initial_data.items:
                    event_queue.put(
                        ("SNAPSHOT", resource_type, item)
                    )

                # WATCH: receive subsequent changes.
                watcher = watch.Watch()

                for event in watcher.stream(
                    list_function,
                    namespace=namespace,
                    resource_version=resource_version,
                    timeout_seconds=300,
                    _request_timeout=330,
                    **filters
                ):

                    if stop_event.is_set():
                        watcher.stop()
                        break

                    event_type = event["type"]
                    obj = event["object"]

                    # Resource version expired.
                    if event_type == "ERROR":

                        if getattr(obj, "code", None) == 410:
                            event_queue.put((
                                "INFO",
                                resource_type,
                                "Resource version expired. Relisting..."
                            ))
                            watcher.stop()
                            break

                        event_queue.put((
                            "ERROR",
                            resource_type,
                            str(getattr(obj, "message", obj))
                        ))

                        watcher.stop()
                        break

                    event_queue.put(
                        (event_type, resource_type, obj)
                    )

                watcher.stop()

            except ApiException as error:

                if error.status == 410:
                    event_queue.put((
                        "INFO",
                        resource_type,
                        "Resource version expired. Relisting..."
                    ))

                elif error.status in (401, 403):
                    event_queue.put((
                        "ERROR",
                        resource_type,
                        f"Authentication/permission error: "
                        f"{error.status} {error.reason}"
                    ))
                    break

                else:
                    event_queue.put((
                        "ERROR",
                        resource_type,
                        f"API error: {error.status} {error.reason}"
                    ))

            except Exception as error:
                event_queue.put((
                    "ERROR",
                    resource_type,
                    str(error)
                ))

            # Avoid tight reconnect loops.
            stop_event.wait(2)

    finally:
        if api_client:
            api_client.close()


# --------------------------------------------------
# MONITOR JOB USING WATCH API
# --------------------------------------------------

def monitor_job(cluster):
    namespace = cluster["namespace"]
    job_name = input("\nEnter Job ID: ").strip()

    if not job_name:
        print("Job ID is required.")
        return

    kubeconfig_path = BASE_DIR / cluster["kubeconfig"]

    if not kubeconfig_path.is_file():
        print("Kubeconfig not found.")
        return

    event_queue = queue.Queue()
    stop_event = threading.Event()

    print("\n========== JOB MONITOR ==========")
    print("Job ID:", job_name)
    print("Namespace:", namespace)
    print("Monitoring using Kubernetes Watch API...")
    print("\nPress Ctrl+C to stop monitoring.\n")

    # One thread watches the Job.
    job_thread = threading.Thread(
        target=watch_resource,
        args=(
            kubeconfig_path,
            namespace,
            "JOB",
            job_name,
            event_queue,
            stop_event
        ),
        daemon=True
    )

    # Another thread watches the Job's Pods.
    pod_thread = threading.Thread(
        target=watch_resource,
        args=(
            kubeconfig_path,
            namespace,
            "POD",
            job_name,
            event_queue,
            stop_event
        ),
        daemon=True
    )

    job_thread.start()
    pod_thread.start()

    job_state = "PENDING"
    pod_states = {}

    try:
        while True:

            try:
                event_type, resource_type, obj = (
                    event_queue.get(timeout=1)
                )

            except queue.Empty:
                if not job_thread.is_alive() and not pod_thread.is_alive():
                    print("\nBoth Watch streams have stopped.")
                    break
                continue

            if event_type == "INFO":
                print(f"[{resource_type}] {obj}")
                continue

            if event_type == "ERROR":
                print(f"[{resource_type} ERROR] {obj}")
                continue

            if resource_type == "JOB":

                new_state = get_job_state(obj)

                job_state = new_state

                print("\n----------------------------------")
                print("JOB UPDATE")
                print("Job ID:", obj.metadata.name)
                print("Event:", event_type)
                print("Status:", job_state)
                print("Active:", obj.status.active or 0)
                print("Succeeded:", obj.status.succeeded or 0)
                print("Failed:", obj.status.failed or 0)

                if job_state in ("COMPLETED", "FAILED"):
                    print("\nJob execution finished.")
                    stop_event.set()
                    break

            elif resource_type == "POD":

                pod_name = obj.metadata.name
                pod_state = get_pod_state(obj)

                pod_states[pod_name] = pod_state

                print("\nPOD UPDATE")
                print("Pod:", pod_name)
                print("Event:", event_type)
                print("Status:", pod_state)

                if obj.status:
                    print(
                        "Node:",
                        obj.spec.node_name or "Not scheduled"
                    )

        print("\n========== FINAL STATUS ==========")
        print("Job ID:", job_name)
        print("Job Status:", job_state)

        print("\nPod Status Summary:")

        if not pod_states:
            print("No Pod events received.")
        else:
            for pod_name, state in pod_states.items():
                print(f"{pod_name} -> {state}")

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")

    finally:
        stop_event.set()

        job_thread.join(timeout=3)
        pod_thread.join(timeout=3)

        print("\nWatch monitoring stopped.")


# --------------------------------------------------
# LIST EXISTING JOBS
# --------------------------------------------------

def list_jobs(api_client, cluster):
    batch_api = client.BatchV1Api(api_client)

    try:
        jobs = batch_api.list_namespaced_job(
            namespace=cluster["namespace"],
            label_selector="app=job-orchestrator"
        )

        print("\n========== EXISTING JOBS ==========")

        if not jobs.items:
            print("No Jobs found.")
            return

        for job in jobs.items:
            print(
                f"{job.metadata.name} | "
                f"{get_job_state(job)}"
            )

    except ApiException as error:
        print("\nOpenShift API Error:")
        print(error.status, error.reason)


# --------------------------------------------------
# MAIN APPLICATION
# --------------------------------------------------

def main():

    try:
        configuration = load_configuration()
    except (FileNotFoundError, json.JSONDecodeError) as error:
        print("Configuration error:", error)
        return

    while True:

        org_id, organization = select_organization(
            configuration
        )

        if not organization:
            continue

        cluster_id, cluster = select_cluster(
            organization
        )

        if not cluster:
            continue

        print("\nConnecting to cluster:", cluster_id)

        api_client = None

        try:
            api_client = connect_cluster(cluster)

            version = client.VersionApi(
                api_client
            ).get_code()

            print("\nConnected successfully.")
            print("Kubernetes API version:", version.git_version)
            print("Organization:", org_id)
            print("Cluster:", cluster_id)
            print("Namespace:", cluster["namespace"])

            while True:

                print("\n========== MAIN MENU ==========")
                print("1. Create and Execute Job")
                print("2. Monitor Job (Watch API)")
                print("3. List Existing Jobs")
                print("4. Change Organization / Cluster")
                print("5. Exit")

                choice = input("\nEnter choice: ").strip()

                if choice == "1":
                    create_job(api_client, cluster, org_id)

                elif choice == "2":
                    monitor_job(cluster)

                elif choice == "3":
                    list_jobs(api_client, cluster)

                elif choice == "4":
                    break

                elif choice == "5":
                    print("Exiting application.")
                    return

                else:
                    print("Invalid choice.")

        except (FileNotFoundError, ApiException) as error:
            print("\nConnection/Configuration Error:", error)

        except KeyboardInterrupt:
            print("\nReturning to organization selection.")

        finally:
            if api_client:
                api_client.close()


if __name__ == "__main__":
    main()
