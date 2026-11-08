
import json
import uuid
import time

from pathlib import Path
from kubernetes import client, config
from kubernetes.client.rest import ApiException


BASE_DIR = Path(__file__).resolve().parent

CONFIG_FILE = BASE_DIR / "clusters.json"


# --------------------------------------------------
# LOAD ORGANIZATION AND CLUSTER CONFIGURATION
# --------------------------------------------------

def load_configuration():

    with open(CONFIG_FILE, "r") as file:
        return json.load(file)


# --------------------------------------------------
# SELECT ORGANIZATION
# --------------------------------------------------

def select_organization(configuration):

    organizations = configuration["organizations"]

    org_ids = list(organizations.keys())

    print("\n========== SELECT ORGANIZATION ==========")

    for index, org_id in enumerate(org_ids, 1):
        print(f"{index}. {org_id}")

    choice = int(input("\nSelect organization: "))

    if choice < 1 or choice > len(org_ids):
        print("Invalid selection.")
        return None, None

    org_id = org_ids[choice - 1]

    return org_id, organizations[org_id]


# --------------------------------------------------
# SELECT CLUSTER
# --------------------------------------------------

def select_cluster(organization):

    clusters = organization["clusters"]

    cluster_ids = list(clusters.keys())

    print("\n========== SELECT CLUSTER ==========")

    for index, cluster_id in enumerate(cluster_ids, 1):
        print(f"{index}. {cluster_id}")

    choice = int(input("\nSelect cluster: "))

    if choice < 1 or choice > len(cluster_ids):
        print("Invalid selection.")
        return None, None

    cluster_id = cluster_ids[choice - 1]

    return cluster_id, clusters[cluster_id]


# --------------------------------------------------
# CONNECT TO OPENSHIFT
# --------------------------------------------------

def connect_cluster(cluster):

    kubeconfig_path = BASE_DIR / cluster["kubeconfig"]

    if not kubeconfig_path.is_file():
        raise FileNotFoundError(
            f"Kubeconfig not found: {kubeconfig_path}"
        )

    api_client = config.new_client_from_config(
        config_file=str(kubeconfig_path)
    )

    return api_client


# --------------------------------------------------
# CREATE AND EXECUTE JOB
# --------------------------------------------------

def create_job(api_client, cluster):

    namespace = cluster["namespace"]

    image = "busybox:latest"
    command =  "echo 'Hello' && sleep 10 && echo 'Job completed.'"
    job_name = "job-" + uuid.uuid4().hex[:8]

    container = client.V1Container(
        name="job-container",
        image=image,
        command=["sh", "-c", command]
    )

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        containers=[container]
    )

    pod_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(
            labels={"app": "job-orchestrator"}
        ),
        spec=pod_spec
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name
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
        print("Namespace:", namespace)
        print("Image:", image)
        print("Status: SUBMITTED")

        return response.metadata.name

    except ApiException as error:

        print("\nOpenShift API Error:")
        print(error.status, error.reason)

    return None


# --------------------------------------------------
# MONITOR JOB
# --------------------------------------------------

def monitor_job(api_client, cluster):

    namespace = cluster["namespace"]

    job_name = input("\nEnter Job ID: ").strip()

    if not job_name:
        return

    batch_api = client.BatchV1Api(api_client)
    core_api = client.CoreV1Api(api_client)

    print("\nMonitoring Job:", job_name)

    try:

        while True:

            job = batch_api.read_namespaced_job(
                name=job_name,
                namespace=namespace
            )

            status = job.status

            if status.succeeded:
                job_status = "COMPLETED"

            elif status.failed:
                job_status = "FAILED"

            elif status.active:
                job_status = "RUNNING"

            else:
                job_status = "PENDING"

            pods = core_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}"
            )

            print("\n----------------------------------")
            print("Job ID:", job_name)
            print("Job Status:", job_status)
            print("Active:", status.active or 0)
            print("Succeeded:", status.succeeded or 0)
            print("Failed:", status.failed or 0)

            print("\nPod Status:")

            for pod in pods.items:

                print(
                    pod.metadata.name,
                    "->",
                    pod.status.phase
                )

            if job_status in ["COMPLETED", "FAILED"]:
                print("\nJob execution finished.")
                break

            time.sleep(3)

    except ApiException as error:

        print("\nOpenShift API Error:")
        print(error.status, error.reason)


# --------------------------------------------------
# LIST JOBS
# --------------------------------------------------

def list_jobs(api_client, cluster):

    batch_api = client.BatchV1Api(api_client)

    try:

        jobs = batch_api.list_namespaced_job(
            namespace=cluster["namespace"]
        )

        print("\n========== EXISTING JOBS ==========")

        if not jobs.items:
            print("No Jobs found.")
            return

        for job in jobs.items:

            status = job.status

            if status.succeeded:
                state = "COMPLETED"

            elif status.failed:
                state = "FAILED"

            elif status.active:
                state = "RUNNING"

            else:
                state = "PENDING"

            print(
                f"{job.metadata.name} | {state}"
            )

    except ApiException as error:

        print("\nOpenShift API Error:")
        print(error.status, error.reason)


# --------------------------------------------------
# MAIN APPLICATION
# --------------------------------------------------

def main():

    configuration = load_configuration()

    while True:

        print("\n======================================")
        print("       OPENSHIFT JOB ORCHESTRATOR")
        print("======================================")

        try:

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

            api_client = connect_cluster(cluster)

            try:

                version = client.VersionApi(
                    api_client
                ).get_code()

                print("Connected successfully.")
                print("OpenShift Kubernetes API version:", version.git_version)

                while True:

                    print("\n========== MAIN MENU ==========")
                    print("1. Create and Execute Job")
                    print("2. Monitor Job")
                    print("3. List Existing Jobs")
                    print("4. Change Organization / Cluster")
                    print("5. Exit")

                    choice = input("\nEnter choice: ").strip()

                    if choice == "1":

                        create_job(api_client, cluster)

                    elif choice == "2":

                        monitor_job(api_client, cluster)

                    elif choice == "3":

                        list_jobs(api_client, cluster)

                    elif choice == "4":

                        break

                    elif choice == "5":

                        print("Exiting application.")
                        return

                    else:

                        print("Invalid choice.")

            finally:

                api_client.close()

        except (FileNotFoundError, ValueError, ApiException) as error:

            print("\nConnection/Configuration Error:", error)

            input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()
