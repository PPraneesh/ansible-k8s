
import json
import subprocess
import tempfile
import os
from pathlib import Path

# Load organization-to-cluster mapping
with open("clusters.json", "r") as file:
    clusters = json.load(file)


def execute_job(organization_id, job_name, job_image):

    # 1. Find the organization's cluster
    cluster = clusters.get(organization_id)

    if not cluster:
        print("Organization not registered!")
        return

    # 2. Prepare execution parameters
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    variables = {
      "kubeconfig": os.path.join(BASE_DIR, cluster["kubeconfig"]),
      "namespace": cluster["namespace"],
      "job_name": job_name,
      "job_image": job_image
    }

    print("Kubeconfig:", variables["kubeconfig"])
    print("Exists:", os.path.exists(variables["kubeconfig"]))

    # 3. Create a temporary variables file
    temp_file = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False
        ) as file:

            json.dump(variables, file)
            temp_file = file.name

        os.chmod(temp_file, 0o600)

        # 4. Invoke Ansible
        command = [
            "ansible-playbook",
            "-i", "localhost,",
            "playbooks/run-job.yml",
            "--connection=local",
            "--extra-vars", f"@{temp_file}"
        ]

        print(f"\nExecuting job for {organization_id}")
        print(f"Image: {job_image}")
        print(f"Cluster: {cluster['kubeconfig']}\n")

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=950
        )

        # 5. Display execution result
        print(result.stdout)

        if result.returncode == 0:
            print("JOB EXECUTION SUCCESSFUL")
        else:
            print("JOB EXECUTION FAILED")
            print(result.stderr)

    except subprocess.TimeoutExpired:
        print("Execution timed out!")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)


# Main program
if __name__ == "__main__":

    print("EXTERNAL JOB EXECUTION MANAGER")
    print("--------------------------------")

    organization_id = input("Organization ID: ")
    job_name = input("Job name: ")
    job_image = input("Docker image: ")

    execute_job(
        organization_id,
        job_name,
        job_image
    )

