import requests
import time
from datetime import datetime, timezone

def create_environment(workspace_id, access_token, display_name, description):
    """
    Creates an environment in a given workspace in Microsoft Fabric.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        display_name (str): The display name of the environment.
        description (str): A description for the environment.

    Returns:
        str: The ID of the created environment.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "displayName": display_name,
        "description": description
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("id", "").strip()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error occurred while creating environment: {str(e)}")


def publish_environment(workspace_id, artifact_id, access_token):
    """
    Publishes the environment after uploading libraries.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment artifact.
        access_token (str): The service principal access token for authentication.

    Returns:
        dict: The response from the API confirming the publish operation.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}/staging/publish"

    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error publishing environment: {str(e)}")


def poll_environment_publish_status(workspace_id, artifact_id, access_token, polling_interval=60, maximum_duration=1200):
    """
    Polls the environment publish status at given intervals.
    Uses a 5-minute interval for the first 10 minutes, then switches to the provided polling_interval.
    Stops polling as soon as the status changes from 'Running' or when the maximum duration is exceeded.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment.
        access_token (str): The service principal access token for authentication.
        polling_interval (int): Time in seconds between each poll after the first 10 minutes (default: 60 seconds).
        maximum_duration (int): Maximum duration in seconds to poll (default: 1200 seconds).

    Returns:
        str: The publish state once it is no longer 'Running'.

    Raises:
        Exception: If the maximum polling duration is exceeded without a state change.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    elapsed_time = 0

    while elapsed_time < maximum_duration:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()

            # Extract the current publish state from the API response
            current_state = result.get("properties", {}).get("publishDetails", {}).get("state", None)

            # Stop polling if the state is not 'Running'
            if current_state != "Running":
                return current_state

        except Exception as e:
            raise(f"Error getting environment publish status: {str(e)}")

        # Use a 5-minute interval for the first 10 minutes, then the provided polling_interval
        if elapsed_time < 600:
            sleep_interval = 300  # 5 minutes
        else:
            sleep_interval = polling_interval

        time.sleep(sleep_interval)
        elapsed_time += sleep_interval

    raise Exception("Maximum polling duration exceeded without status change.")


def update_default_environment(workspace_id, access_token, environment_name, runtime_version):
    """
    Updates the default environment settings in a workspace.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        environment_name (str): The name of the environment.
        runtime_version (str): The runtime version to set.

    Returns:
        dict: The response from the API confirming the update.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/settings"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "environment": {
            "name": environment_name,
            "runtimeVersion": runtime_version
        }
    }

    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error updating default environment: {str(e)}")


def deploy_custom_environment(workspace_id, access_token):
    """
    Deploys a custom environment to a workspace, including creating the environment, 
    uploading a library, publishing the environment, and setting it as the default environment.

    Parameters:
    - workspace_id (str): The ID of the workspace.
    - access_token (str): The service principal access token to authenticate with the workspace.

    Raises:
        Exception: If an error occurs during any of the deployment steps, an exception is raised.
    """
    try:
        env_name = "Spark_Environment"
        artifact_deployment_time = datetime.now(timezone.utc)
        # Create the environment
        artifact_id = create_environment(workspace_id, access_token, env_name, None)

        # Publish the environment
        publish_environment(workspace_id, artifact_id, access_token)

        # Check publish status
        publish_status = poll_environment_publish_status(workspace_id, artifact_id, access_token)

        if publish_status.lower() == "success":
            # Set the environment as the default
            update_default_environment(workspace_id, access_token, env_name, "1.3")
        else:
            raise Exception("Error in publishing environment.")
    except Exception as e:
        error_message = f"Error occurred while deploying custom environment: {str(e)}"
        raise Exception(error_message)