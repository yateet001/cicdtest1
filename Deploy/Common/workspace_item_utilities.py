import re
import os
import json
import base64
import requests
import time
import traceback
import pandas as pd
from collections import defaultdict, deque
from datetime import datetime, timezone
from workspace_utilities import *
from spark_utilities import *


def list_workspace_all_items(workspace_id, spn_access_token):
    """
    List all the items in the workspace

    parameters:
    - workspace_id: GUID of workspace in which items needs to be listed
    - spn_access_token: Token for authentication in API calls

    returns:
    - list: A list containing items of the workspace
    """

    try:
        api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        headers = {
                "Authorization": f"Bearer {spn_access_token}",  
                "Content-Type": "application/json"  
            }

        response = requests.get(api_url, headers=headers)  

        if response.status_code != 200:
            raise Exception(f"Unable to list items: {str(response.text)}")
        return response.json()["value"]

    except Exception as e:
        raise Exception(f"An error occurred while getting the list of items from workspace '{workspace_id}': {str(e)}")


def get_kusto_uri(workspace_id, database_name, token):
    """
    Fetches the Kusto URI for the specified eventhouse from the workspace.

    Parameters:
    - workspace_id (str): The ID of the workspace.
    - database_name (str): The display name of the eventhouse.
    - token (str): The authentication token.

    Returns:
    str: The Kusto URI for the specified eventhouse.
    """

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/eventhouses"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the GET request
    response = requests.get(url, headers=headers)

    # Check for successful response
    if response.status_code == 200:
        eventhouses = response.json().get("value", [])
        
        # Find the eventhouse with the matching display name
        matching_eventhouse = next(
            (item for item in eventhouses if item["displayName"].strip().lower() == database_name.strip().lower()),
            None
        )

        if matching_eventhouse:
            return matching_eventhouse["properties"]["queryServiceUri"]
        else:
            raise Exception(f"Eventhouse '{database_name}' not found in workspace '{workspace_id}'.")
    else:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")


def add_old_suffix_to_items(workspace_id, items, access_token):
    """
    Add an "_Old" suffix to the display names of specified items in a workspace.

    parameters:
    - workspace_id: GUID of the workspace where the items are located.
    - items: A list of dictionaries representing items, each containing keys like "type", "displayName", "id", and "description".
    - access_token: Token for authentication in API calls.

    returns:
    - bool: True if all items are successfully updated.

    raises:
    - Exception: If an error occurs during the API request or while processing the items.
    """

    try:
        api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/"
        headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"  
            }

        i = 0
        for item in items:

            if i%30 == 0:
                time.sleep(55)
            i = i + 1

            if item.get("type").lower() in ["notebook", "datapipeline"]:
                item_name = item.get("displayName")
             
                item_id = item.get("id")
                payload = {
                        "displayName": item_name + "_Old",
                        "description": item.get("description")
                        }
                
                response = requests.patch(f"{api_url}{item_id}", headers=headers, json=payload)

                if response.status_code != 200:
                    raise Exception(f"An error occurred while renaming item '{item_name}' to '{item_name}_Old': {str(response.text)}")

        return True 
    except Exception as e:
        raise Exception(f"An error occurred while renaming items: {str(e)}")
    

def delete_old_items(workspace_id, items, artifact_path, target_folder, access_token):
    """
    Delete specified items of type "datapipeline" or "notebook" from a workspace.

    parameters:
    - workspace_id: GUID of the workspace from which items need to be deleted.
    - items: A list of dictionaries representing items, each containing keys like "type" and "id".
    - access_token: Token for authentication in API calls.

    returns:
    - bool: True if all specified items are successfully deleted.

    raises:
    - Exception: If an error occurs during the API request or while processing the items.
    """
    
    try:
        api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/"
        headers = {
            "Authorization": f"Bearer {access_token}",  
            "Content-Type": "application/json"
        }

        i = 0
        for item in items:
            if i % 30 == 0:
                time.sleep(55)
            i += 1

            item_name = item.get("displayName")
            if item.get("type").lower() in ["environment"]:
                item_id = item.get("id")
                
                response = requests.delete(f"{api_url}{item_id}", headers=headers)

                if response.status_code != 200:
                    raise Exception(f"An error occurred while deleting item '{item_name}': {str(response.text)}")

        # Get the unsorted pipeline dictionary and item_type by calling the new function
        unsorted_pipeline_dict, item_type = get_unsorted_pipeline_dict(artifact_path, target_folder)

        # Sort the data pipelines based on the publish order
        publish_order = sort_datapipelines(unsorted_pipeline_dict, "Repository", item_type, artifact_path, target_folder, workspace_id, access_token)

        # Reverse the publish order
        publish_order_reversed = publish_order[::-1]

        # Create a hashmap (dictionary) for quick lookup of items by displayName
        items_map = {item["displayName"]: item for item in items}

        i = 0
        for pipeline_name in publish_order_reversed:
            if i % 30 == 0:
                time.sleep(55)
            i += 1

            # Lookup the matching item using the hashmap
            matching_item = items_map.get(pipeline_name)
            if matching_item is None:
                continue
                
            if matching_item.get("type").lower() == "datapipeline":
                item_id = matching_item.get("id")
                # Delete the item using API
                response = requests.delete(f"{api_url}{item_id}", headers=headers)
                if response.status_code != 200:
                    raise Exception(f"An error occurred while deleting item '{pipeline_name}': {str(response.text)}")

        return True 
    except Exception as e:
        raise Exception(f"An error occurred while deleting items: {str(e)}")


def update_notebook_content(notebook_content, lakehouse_dict, workspace_id, target_folder):
    """
    Updates the lakehouse details (name, ID, workspace ID) in the notebook content.
    The update will only be performed if the 'default_lakehouse_name' is empty.

    Parameters:
    notebook_content (str): The content of the notebook to be updated.
    lakehouse_dict (dict): A dictionary containing lakehouse names as keys and their corresponding IDs as values.
    workspace_id (str): The workspace ID to be added to the notebook content.

    Returns:
    str: The updated notebook content with the lakehouse details and workspace ID, if the update was necessary.
    """
    
    # Create a variable to hold the notebook content
    updated_notebook_content = notebook_content
    
    # Extract current lakehouse name from notebook content (if available)
    current_lakehouse_name_match = re.search(r'"default_lakehouse_name": "(.*?)"', updated_notebook_content)

    # Applicable for Operations, Security and Data Engineering layers: if Lakehouse is attached then store its name else skip
    if current_lakehouse_name_match:
        current_lakehouse_name = current_lakehouse_name_match.group(1)
    else:
        current_lakehouse_name = ""

    # For Bronze and Non-Security layers, attach Bronze Lakehouse in all Notebooks
    if (
            (not current_lakehouse_name or current_lakehouse_name not in lakehouse_dict)
            and
            ("Data_Ingestion" in target_folder or "Data_Non_Security" in target_folder)
        ):
        current_lakehouse_name = "Bronze"

    lakehouse_name = current_lakehouse_name

    # Only update content if lakehouse_name is not empty
    if current_lakehouse_name != "": 
        # Get the corresponding lakehouse ID from the dictionary
        lakehouse_id = lakehouse_dict.get(lakehouse_name)

        # Only inject if default_lakehouse isn't already present
        if not re.search(r'"default_lakehouse"\s*:', updated_notebook_content):
            # Insert the default_lakehouse line right before default_lakehouse_name
            updated_notebook_content = re.sub(
                r'(# META\s+"default_lakehouse_name")',
                rf'# META       "default_lakehouse": "{lakehouse_id}",\n\1',
                updated_notebook_content,
                count=1
            )

        # Update the content using regex replacement
        updated_notebook_content = re.sub(r'"default_lakehouse": ("[^"]*"|null)', f'"default_lakehouse": "{lakehouse_id}"', updated_notebook_content)
        updated_notebook_content = re.sub(r'"default_lakehouse_name": ("[^"]*"|null)', f'"default_lakehouse_name": "{lakehouse_name}"', updated_notebook_content)
        updated_notebook_content = re.sub(r'"default_lakehouse_workspace_id": ("[^"]*"|null)', f'"default_lakehouse_workspace_id": "{workspace_id}"', updated_notebook_content)
        pattern = r'"known_lakehouses"\s*:\s*\[[^\]]*\]'
        replacement = f'"known_lakehouses": [{{"id": "{lakehouse_id}"}}]'
        updated_notebook_content = re.sub(pattern, replacement, updated_notebook_content, flags=re.DOTALL)

    # Return the updated notebook content
    return updated_notebook_content


def get_connection_id_by_connection_name(access_token, connection_name):
    """
    Fetches the connection details from the specified API and filters the connections
    based on the provided connection name.

    Parameters:
    access_token (str): The authorization token to authenticate the API request.
    connection_name (str): The name of the connection to filter by.
    Returns:
    connection id: The connection id that match the provided connection name.
    None: If no connection is found.
    
    Raises:
    Exception: If the API request fails, or an error occurs during processing.
    """

    # Define the API URL inside the function
    api_url = "https://api.fabric.microsoft.com/v1/connections"
   
    try:
        # Define the headers with the authorization token
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
 
        # Send a GET request to fetch the list of connections from the API
        response = requests.get(api_url, headers=headers)
       
        # Check if the response status code is 200 (successful request)
        if response.status_code == 200:
            connections = response.json().get("value", [])
 
            # Filter the connections to find the one with the given connection name
            for connection in connections:
                if connection.get("displayName") == connection_name:
                    return connection["id"]  # Return the connection ID
 
            # If no connection with the given connection name is found
            return None
        else:
            # If the response status code is not 200, raise an exception
            raise Exception(f"Failed to fetch connections. Status code: {response.status_code} {response.text}")
    
    except Exception as e:
        # Raise the exception to handle it further up the call stack
        raise Exception(f"An error occurred while fetching connection details for '{connection_name}': {str(e)}")


def filter_lakehouses(artifact_path, target_folder):
    """
    Filters and returns the paths of 'lakehouse.metadata.json' and platform files from the specified folder in the artifact.

    Parameters:
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.

    Returns:
    tuple: A tuple containing two lists:
        - lakehouses (list): A list of paths to 'lakehouse.metadata.json' files.
        - lh_platforms (list): A list of paths to '.platform' files within Lakehouse subdirectories.
    """
    target_folder_path = os.path.join(artifact_path, target_folder)
    lakehouses = []
    lh_platforms = []
    
    try:
        # Check if the target folder exists
        if not os.path.exists(target_folder_path):
            # Try alternative case variations if the original doesn't exist
            alternative_paths = []
            base_path = os.path.dirname(target_folder_path)
            folder_name = os.path.basename(target_folder_path)
            
            if os.path.exists(base_path):
                # List all directories in the base path and find case-insensitive matches
                for item in os.listdir(base_path):
                    if item.lower() == folder_name.lower() and os.path.isdir(os.path.join(base_path, item)):
                        alternative_paths.append(os.path.join(base_path, item))
            
            if alternative_paths:
                target_folder_path = alternative_paths[0]
                print(f"Warning: Target folder '{target_folder}' not found. Using '{target_folder_path}' instead.")
            else:
                # If no matching folder is found, return empty lists instead of raising exception
                print(f"Warning: Target folder '{target_folder}' does not exist in the artifact. No lakehouses to deploy.")
                return lakehouses, lh_platforms

        # Traverse the directory to find 'lakehouse.metadata.json' and platform files
        for root, _, files in os.walk(target_folder_path):
            # Check if the root contains '.Lakehouse' before proceeding to loop through the files
            if ".Lakehouse" in root:
                for file in files:
                    if file == "lakehouse.metadata.json":
                        lakehouses.append(os.path.join(root, file))
                    elif file == ".platform":
                        lh_platforms.append(os.path.join(root, file))
                        
    except Exception as e:
        print(f"Warning: An error occurred while filtering lakehouses: {str(e)}")
        return [], []  # Return empty lists instead of raising exception

    return lakehouses, lh_platforms

def filter_notebooks(artifact_path, target_folder):
    """
    Filters and returns the paths of 'notebook-content.py' and platform files from the specified folder in the artifact.

    Parameters:
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.

    Returns:
    tuple: A tuple containing two lists:
        - notebooks (list): A list of paths to 'notebook-content.py' files.
        - platforms (list): A list of paths to '.platform' files within Notebook subdirectories.
    """
    target_folder_path = os.path.join(artifact_path, target_folder)
    notebooks = []
    platforms = []

    try:
        # Check if the target folder exists
        if not os.path.exists(target_folder_path):
            # Try alternative case variations
            alternative_paths = []
            base_path = os.path.dirname(target_folder_path)
            folder_name = os.path.basename(target_folder_path)
            
            if os.path.exists(base_path):
                for item in os.listdir(base_path):
                    if item.lower() == folder_name.lower() and os.path.isdir(os.path.join(base_path, item)):
                        alternative_paths.append(os.path.join(base_path, item))
            
            if alternative_paths:
                target_folder_path = alternative_paths[0]
                print(f"Warning: Target folder '{target_folder}' not found. Using '{target_folder_path}' instead.")
            else:
                print(f"Warning: Target folder '{target_folder}' does not exist in the artifact. No notebooks to deploy.")
                return notebooks, platforms

        # Traverse the directory to find 'notebook-content.py' and platform files
        for root, _, files in os.walk(target_folder_path):
            # Check if the root contains '.Notebook' before proceeding to loop through the files
            if ".Notebook" in root:
                for file in files:
                    if file == "notebook-content.py":
                        notebooks.append(os.path.join(root, file))
                    elif file == ".platform":
                        platforms.append(os.path.join(root, file))

    except Exception as e:
        print(f"Warning: An error occurred while filtering notebooks: {str(e)}")
        return [], []

    return notebooks, platforms

def filter_pipelines(artifact_path, target_folder):
    """
    Filters and returns the paths of 'pipeline-content.json' and platform files from the specified folder in the artifact.

    Parameters:
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.

    Returns:
    tuple: A tuple containing two lists:
        - pipelines (list): A list of paths to 'pipeline-content.json' files.
        - platforms (list): A list of paths to '.platform' files within DataPipeline subdirectories.
    """
    target_folder_path = os.path.join(artifact_path, target_folder)
    pipelines = []
    platforms = []

    try:
        # Check if the target folder exists
        if not os.path.exists(target_folder_path):
            # Try alternative case variations
            alternative_paths = []
            base_path = os.path.dirname(target_folder_path)
            folder_name = os.path.basename(target_folder_path)
            
            if os.path.exists(base_path):
                for item in os.listdir(base_path):
                    if item.lower() == folder_name.lower() and os.path.isdir(os.path.join(base_path, item)):
                        alternative_paths.append(os.path.join(base_path, item))
            
            if alternative_paths:
                target_folder_path = alternative_paths[0]
                print(f"Warning: Target folder '{target_folder}' not found. Using '{target_folder_path}' instead.")
            else:
                print(f"Warning: Target folder '{target_folder}' does not exist in the artifact. No pipelines to deploy.")
                return pipelines, platforms

        # Traverse the directory to find 'pipeline-content.json' and platform files
        for root, _, files in os.walk(target_folder_path):
            # Check if the root contains '.DataPipeline' before proceeding to loop through the files
            if ".DataPipeline" in root:
                for file in files:
                    if file == "pipeline-content.json":
                        pipelines.append(os.path.join(root, file))
                    elif file == ".platform":
                        platforms.append(os.path.join(root, file))

    except Exception as e:
        print(f"Warning: An error occurred while filtering pipelines: {str(e)}")
        return [], []

    return pipelines, platforms


def create_lakehouse(spn_access_token, workspace_id, lakehouse_name):
    """
    Sends a request to the Create Item API to create a lakehouse in the target workspace.

    Parameters:
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the lakehouse will be created.
    lakehouse_name (str): The prefix for the lakehouse name.

    Returns:
    dict: The response data from the API request, containing information about the created lakehouse.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    
    headers = {
        "Authorization": f"Bearer {spn_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "displayName": lakehouse_name,
        "type": "Lakehouse",
    }

    try:
        start_time=datetime.now(timezone.utc)
        # Send the POST request to create the lakehouse
        response = requests.post(url, headers=headers, json=payload)
        
        response_data = response.json()

        if response.status_code == 201:
            return response_data
        elif response.status_code == 202:
            return response_data
        else:
            raise Exception(f"Failed to create lakehouse '{lakehouse_name}': {response.status_code} - {response.text}")
        
    except Exception as e:
        # Handle other unexpected errors
        error_message = str(e)

        raise Exception(f"Request error while creating lakehouse: {error_message}")

def create_notebook(spn_access_token, workspace_id, workspace_name, notebook_name, notebook_content, notebook_path, platform_content, platform_path, existing_items, guids):
    """
    Sends a request to the Create Item API to create a notebook in the target workspace.

    Parameters:
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the notebook will be created.
    notebook_name (str): The name of the notebook to be created.
    content (str): The content of the notebook in base64 format.
    notebook_path (str): The path of the notebook content.
    platform_content (str): The platform content associated with the notebook.
    platform_path (str): The path of the platform content.
    existing_items (dict) : A dictionary containing all the existing items of the workspace
    guids(list):

    Returns:
    dict: The response data from the API request, containing information about the created notebook.
    """

    notebook_key = f"{notebook_name}.Notebook"
    headers = {
            "Authorization": f"Bearer {spn_access_token}",
            "Content-Type": "application/json"
        }
    notebook_description = ""
    # Base64 encode the notebook and platform content to send it as a payload
    try:
        start_time=datetime.now(timezone.utc)
        encoded_notebook_content = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")
        encoded_platform_content = base64.b64encode(platform_content.encode("utf-8")).decode("utf-8")
        
        # Parse the JSON string
        platform_content_json = json.loads(platform_content)

        # Check if 'description' key exists in the metadata before accessing it
        if "description" in platform_content_json["metadata"]:
            notebook_description = platform_content_json["metadata"]["description"]
        else:
            # Set a default value blank string
            notebook_description = ""

        payload = {
            "definition": {
                "parts": [
                    {
                        "path": notebook_path,
                        "payload": encoded_notebook_content,
                        "payloadType": "InlineBase64"
                    },
                    {
                        "path": platform_path,
                        "payload": encoded_platform_content,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        if notebook_key in existing_items:           
            # Update existing notebook
            notebook_id = existing_items[notebook_key]["id"]
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/updateDefinition"
        else:
            # Create new notebook
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"

            payload["displayName"]= notebook_name
            payload["type"] = "Notebook"
            payload["description"] = notebook_description[:256]
        
        
        # Send the POST request to create the notebook
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            # Notebook updated in sync
            guids.append({"artifact_type": "Notebook", "artifact_name": notebook_name, "artifact_location_guid": workspace_id, "artifact_location_name": workspace_name, "artifact_guid": existing_items[notebook_key]["id"]})
            return {"message": f"Notebook '{notebook_name}' updated successfully.", "data": response.json()}
        elif response.status_code == 201:
            response_data = response.json()
            guids.append({"artifact_type": "Notebook", "artifact_name": notebook_name, "artifact_location_guid": workspace_id, "artifact_location_name": workspace_name, "artifact_guid": response_data["id"]})
            return {"message": f"Notebook '{notebook_name}' created successfully.", "data": response.json()}
        elif response.status_code == 202:
            response_data = handle_async_creation(response, headers)
            guids.append({"artifact_type": "Notebook", "artifact_name": notebook_name, "artifact_location_guid": workspace_id, "artifact_location_name": workspace_name, "artifact_guid": response_data["id"]})
            return {"message": f"Notebook '{notebook_name}' create request accepted.", "data": response.json()}
        else:
            raise Exception(f"Failed to create notebook '{notebook_name}': {response.status_code} - {response.text}")
        
    except Exception as e:
        # Handle other unexpected errors
        error_message = str(e)
        raise Exception(f"Request error while creating notebook: {error_message}")

    
def handle_async_creation(response, headers):
    """
    Handles polling when a notebook creation request returns a 202 Accepted response.

    Parameters:
    - response: The initial API response containing the Location header.
    - headers: Header for API call authentication

    Returns:
    - dict: The notebook details if creation is successful, None otherwise.
    """
    location_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", 30))  # Default to 30 seconds if not provided
    operation_id = response.headers.get("x-ms-operation-id")

    if not location_url:
        raise Exception("Error: Location header is missing in the response.")

    while True:
        time.sleep(retry_after)
        operation_response = requests.get(location_url, headers=headers)

        if operation_response.status_code == 200:
            operation_status = operation_response.json().get("status")

            if operation_status.lower().strip() == "succeeded":
                return get_operation_result(operation_id, headers)
            elif operation_status.lower().strip() in ["failed", "cancelled"]:
                raise Exception(f"Error: Notebook creation failed with status '{operation_status}'.")
        else:
            raise Exception(f"Error: Unable to check operation status. Status code {operation_response.status_code}")

def get_operation_result(operation_id, headers):
    """
    Retries fetching the notebook definition after operation completion.
    
    Parameters:
    - operation_id: The ID of the notebook.
    - headers:  Headers for authentication.

    Ruturns:
    - dict: The final notebook definition.
    """
    url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}/result"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: Unable to retrieve operation result. Status code {response.status_code}")
    

def replace_logical_ids(raw_file, artifact_path, target_folder, workspace_id, access_token):
    """
    Replaces logical IDs with deployed GUIDs in the raw file content.

    Parameters:
    raw_file (str): The raw file content where logical IDs need to be replaced.
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.
    workspace_id (str): The ID of the workspace where the operation is performed.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    str: The raw file content with logical IDs replaced by GUIDs.
    """

    try:
        # Retrieve the list of repository items
        repository_items = repository_items_list(artifact_path, target_folder, workspace_id, access_token)

        # Iterate over items and replace logical IDs with GUIDs
        for items in repository_items.values():
            for item_dict in items.values():
                logical_id = item_dict.get("logical_id")
                item_guid = item_dict.get("guid")

                # If logical_id and guid are found, perform the replacement
                if logical_id and logical_id in raw_file:
                    if not item_guid:
                        # If item_guid is missing, raise an exception
                        raise Exception(f"Item with logical ID {logical_id} is not yet deployed.")
                    raw_file = raw_file.replace(logical_id, item_guid)
        
        # Replace workspace id "00000000-0000-0000-0000-000000000000" with workspace id
        raw_file = raw_file.replace("00000000-0000-0000-0000-000000000000", workspace_id)

        return raw_file

    except Exception as e:
        # Catch any exceptions raised during the processing and raise them with context
        raise Exception(f"Error during repository items list retrieval or logical ID replacement: {str(e)}")

def repository_items_list(artifact_path, target_path, workspace_id, access_token):
    """
    Scans the artifact directory and returns a dictionary of repository items.

    Parameters:
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.
    workspace_id (str): The ID of the workspace where the pipeline will be created.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    dict: A dictionary containing the repository items found in the artifact directory.
    """
    
    try:
        # Initialize an empty dictionary to store repository items
        repository_items = {}

        # Assuming filter_pipelines returns two lists: pipelines and platforms
        pipelines, platforms = filter_pipelines(artifact_path, target_path)

        # Get the deployed items
        deployed_items = deployed_items_list(workspace_id, access_token)

        for pipeline_path, platform_path in zip(pipelines, platforms):

            item_metadata_path = platform_path

            with open(item_metadata_path, 'r') as file:
                item_metadata = json.load(file)
            
            # Ensure required metadata fields are present
            if "type" not in item_metadata["metadata"] or "displayName" not in item_metadata["metadata"]:
                raise ValueError(f"displayName & type are required in {item_metadata_path}")

            item_type = item_metadata["metadata"]["type"]
            item_description = item_metadata["metadata"].get("description", "")
            item_name = item_metadata["metadata"]["displayName"]
            item_logical_id = item_metadata["config"]["logicalId"]

            # Get the GUID from deployed_items if the item is already deployed (if available)
            item_guid = deployed_items.get(item_type, {}).get(item_name, {}).get("guid", "")

            if item_type not in repository_items:
                repository_items[item_type] = {}

            # Add the item to the repository_items dictionary
            repository_items[item_type][item_name] = {
                "description": item_description,
                "path": pipeline_path,
                "guid": item_guid,
                "logical_id": item_logical_id
            }

        # Retrieve lakehouses and platform paths
        lakehouses, lh_platforms = filter_lakehouses(artifact_path, target_path)

        for lakehouse_path, platform_path in zip(lakehouses, lh_platforms):

            item_metadata_path = platform_path

            try:
                with open(item_metadata_path, "r") as file:
                    item_metadata = json.load(file)
            except Exception as e:
                raise Exception(f"Error reading file {item_metadata_path}: {str(e)}") from e

            # Ensure required metadata fields are present
            if "type" not in item_metadata["metadata"] or "displayName" not in item_metadata["metadata"]:
                raise ValueError(f"displayName & type are required in {item_metadata_path}")

            item_type = item_metadata["metadata"]["type"]
            item_description = item_metadata["metadata"].get("description", "")
            item_name = item_metadata["metadata"]["displayName"]
            item_logical_id = item_metadata["config"]["logicalId"]

            # Get the GUID from deployed_items if the item is already deployed (if available)
            item_guid = deployed_items.get(item_type, {}).get(item_name, {}).get("guid", "")

            if item_type not in repository_items:
                repository_items[item_type] = {}

            # Add the item to the repository_items dictionary
            repository_items[item_type][item_name] = {
                "description": item_description,
                "path": lakehouse_path,
                "guid": item_guid,
                "logical_id": item_logical_id
            }

        # Retrieve notebooks and platforms
        notebooks, platforms = filter_notebooks(artifact_path, target_path)

        for notebook_path, platform_path in zip(notebooks, platforms):

            item_metadata_path = platform_path

            # Attempt to read the metadata file
            try:
                with open(item_metadata_path, 'r') as file:
                    item_metadata = json.load(file)
            except Exception as e:
                raise Exception(f"Error reading file {item_metadata_path}: {str(e)}") from e


            # Ensure required metadata fields are present
            if 'type' not in item_metadata['metadata'] or 'displayName' not in item_metadata['metadata']:
                raise ValueError(f"displayName & type are required in {item_metadata_path}")

            item_type = item_metadata['metadata']['type']
            item_description = item_metadata['metadata'].get('description', '')
            item_name = item_metadata['metadata']['displayName']
            item_logical_id = item_metadata['config']['logicalId']

            # Get the GUID from deployed_items if the item is already deployed (if available)
            item_guid = deployed_items.get(item_type, {}).get(item_name, {}).get("guid", "")

            if item_type not in repository_items:
                repository_items[item_type] = {}

            # Add the item to the repository_items dictionary
            repository_items[item_type][item_name] = {
                "description": item_description,
                "path": notebook_path,  # The path is the artifact path
                "guid": item_guid,
                "logical_id": item_logical_id
            }

        # Return the populated repository_items dictionary
        return repository_items

    except Exception as e:
        # Catch all exceptions and re-raise them with context
        raise Exception(f"Error in repository items list processing: {str(e)}") from e


def deployed_items_list(workspace_id, access_token):
    """
    Queries the Fabric workspace items API to retrieve and return a dictionary of deployed items.

    Parameters:
    workspace_id (str): The ID of the workspace where the pipeline will be created.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    list: A list of deployed items retrieved from the Fabric workspace.
    """

    try:

        # Initialize deployed_items as a dictionary
        deployed_items = {}

        # API URL based on workspace ID
        api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Make the GET request to the API
        response = requests.get(api_url, headers=headers)

        # Check for successful response
        if response.status_code != 200:
            raise Exception(f"Unable to list items: {str(response.text)}")

        # Parse the response JSON
        items = response.json()["value"]

        # Iterate through the API response to populate the deployed_items dictionary
        for item in items:
            item_type = item["type"]
            item_description = item["description"]
            item_name = item["displayName"]
            item_guid = item["id"]

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in deployed_items:
                deployed_items[item_type] = {}

            # Add item details to the deployed_items dictionary
            deployed_items[item_type][item_name] = {
                "description": item_description,
                "guid": item_guid
            }
        # Return the populated deployed_items dictionary
        return deployed_items

    except Exception as e:
        raise Exception(f"An error occurred while getting the list of items from workspace {workspace_id}: {str(e)}")


def convert_id_to_name(item_type, generic_id, lookup_type, artifact_path, target_folder, workspace_id, access_token):
    """
    Returns the item name for a given item type and ID, with special handling for both deployed and repository items.

    Parameters:
    item_type (str): The type of the item (e.g., Notebook, Environment).
    generic_id (str): The logical ID or GUID of the item, depending on the lookup type.
    lookup_type (str): Specifies whether to look up the item in deployed files or repository files (Deployed or Repository).
    artifact_path (str): The path to the artifacts directory.
    target_folder (str): The folder within the artifact path where the items are located.
    workspace_id (str): The ID of the workspace where the pipeline should be created.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    str or None: The name of the item if found, or None if not found.
    """

    try:
        # Retrieve the lookup dictionary based on the lookup_type (Repository or Deployed)
        if lookup_type.strip().lower() == "repository":
            lookup_dict = repository_items_list(artifact_path, target_folder, workspace_id, access_token)
        else:
            lookup_dict = deployed_items_list(workspace_id, access_token)

        # Determine the key to search for based on the lookup_type
        lookup_key = "logical_id" if lookup_type.strip().lower() == "repository" else "guid"
        
        # Search for the item by comparing the given ID with the relevant field in the lookup_dict
        for item_name, item_details in lookup_dict.get(item_type, {}).items():
            if item_details.get(lookup_key) == generic_id:
                return item_name
        
        # Return None if the item is not found
        return None

    except KeyError as e:
        # Handle missing keys in the lookup_dict
        raise Exception(f"KeyError: The expected key '{e}' was not found in the lookup dictionary.")
    except Exception as e:
        # Catch other exceptions and re-raise them with additional context
        raise Exception(f"An error occurred while processing the conversion: {str(e)}") from e


def find_referenced_datapipelines(item_type, item_content_dict, lookup_type, artifact_path, target_folder, workspace_id, access_token):
    """
    Scans through the item path to find references to pipelines, including nested pipeline activities.

    Parameters:
    item_type (str): The type of the item (e.g., Lakehouse, Notebook, DataPipeline).
    item_content_dict (dict): A dictionary representation of the pipeline content file.
    lookup_type (str): Specifies whether to search for references in deployed files or repository files (Deployed or Repository).
    artifact_path (str): The root path of the artifacts directory.
    target_folder (str): The folder (relative to artifact_path) where the artifacts are located.
    workspace_id (str): The ID of the workspace where the pipeline should be created.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    list: A list of pipeline names referenced in the item, including any nested pipeline activities.
    """

    reference_list = []  # To store referenced pipeline names

    def find_execute_pipeline_activities(input_object):
        """
        Recursively scans through JSON to find all pipeline references.

        :param input_object: Object can be a dict or list present in the input JSON.
        """
        try:
            # Check if the current object is a dict
            if isinstance(input_object, dict):
                for key, value in input_object.items():
                    referenced_id = None
                    
                    # Check for legacy and new pipeline activities
                    if key.strip().lower() == "type" and value.strip().lower() == "executepipeline":
                        referenced_id = input_object["typeProperties"]["pipeline"]["referenceName"]
                    elif key.strip().lower() == "type" and value.strip().lower() == "invokepipeline":
                        referenced_id = input_object["typeProperties"]["pipelineId"]
                    
                    # Add found pipeline reference to list
                    if referenced_id is not None:
                        try:
                            referenced_name = convert_id_to_name(item_type=item_type, 
                                                                  generic_id=referenced_id, 
                                                                  lookup_type=lookup_type,  
                                                                  artifact_path=artifact_path,
                                                                  target_folder=target_folder,
                                                                  workspace_id=workspace_id, 
                                                                  access_token=access_token)
                            
                            if referenced_name:
                                reference_list.append(referenced_name)
                        except Exception as e:
                            raise Exception(f"Error in converting ID to name for referenced pipeline {referenced_id}: {str(e)}")
                    
                    # Recursively search in the value
                    else:
                        find_execute_pipeline_activities(value)

            # Check if the current object is a list
            elif isinstance(input_object, list):
                # Recursively search in each item  
                for item in input_object:
                    find_execute_pipeline_activities(item)

        except Exception as e:
            # Raise an exception to propagate error up the call stack
            raise Exception(f"Error in processing pipeline activity: {str(e)}") from e

    try:
        # Start the recursive search from the root of the JSON data
        find_execute_pipeline_activities(item_content_dict)
    except Exception as e:
        raise Exception(f"Error in finding referenced datapipelines: {str(e)}") from e

    return reference_list

def get_connection_name(connections_data, connection_type):
    """Fetch the connection ID based on the connection type."""
    for connection in connections_data.get("Connections", []):
        if connection.get("type") == connection_type:
            return connection.get("connection_name")
    return None


def update_connection_and_workspace_id(raw_json, connections_data, access_token, workspace_id, kql_database_id=None, endpoint=None):
    """
    Recursively traverses a JSON structure and updates the 'connection' field inside 'externalReferences' 
    of 'datasetSettings' with the provided connection ID, and updates the 'workspaceId' inside 'linkedService' 
    of 'datasetSettings' with the provided workspace ID.

    Parameters:
    raw_json (str): The raw JSON string to be modified.
    connections_data (dict): A dictionary of all connection names and types.
    workspace_id (str): The new workspace ID to be used in the 'linkedService' of 'datasetSettings'.

    Returns:
    str: The updated JSON string with the modified connection ID and workspace ID, formatted with indentation.
    """
    # Load the raw JSON into a Python dictionary
    json_data = json.loads(raw_json)

    # Function to recursively search and update both 'connection' and 'workspaceId' inside 'datasetSettings'
    def update_fields(data):
        if isinstance(data, dict):
            # If 'datasetSettings' is found in the dictionary, update fields
            if "datasetSettings" in data:
                dataset_settings = data["datasetSettings"]
                
                # Update connection ID based on type
                if "typeProperties" in dataset_settings and "externalReferences" in dataset_settings:
                    location_type = dataset_settings["typeProperties"].get("location", {}).get("type")
                    dataset_type = dataset_settings.get("type")

                    if location_type == "AzureBlobStorageLocation":
                        connection_id = get_connection_id_by_connection_name(access_token, get_connection_name(connections_data, "Azure Blob Storage"))
                    elif dataset_type == "SqlServerTable":
                        connection_id = get_connection_id_by_connection_name(access_token, get_connection_name(connections_data, "SQL Server"))
                    if connection_id:
                        dataset_settings["externalReferences"]["connection"] = connection_id

                # Update the workspaceId field if 'linkedService' exists
                if "linkedService" in dataset_settings:
                    linked_service = dataset_settings["linkedService"]
                    if "properties" in linked_service:
                        properties = linked_service["properties"]
                        if "typeProperties" in properties:
                            type_properties = properties["typeProperties"]
                            type_properties["workspaceId"] = workspace_id

            if "linkedService" in data:
                linked_service = data["linkedService"]
                if "properties" in linked_service:
                    properties = linked_service["properties"]
                    if properties.get("type") == "KustoDatabase":
                        if "typeProperties" in properties:
                            type_properties = properties["typeProperties"]
                            type_properties["workspaceId"] = workspace_id
                            type_properties["endpoint"] = endpoint
                            type_properties["database"] = kql_database_id

            # Recursively check all keys and nested dictionaries
            for key, value in data.items():
                update_fields(value)
        elif isinstance(data, list):
            # If the data is a list, iterate through each element
            for item in data:
                update_fields(item)

    # Call the recursive function to traverse the JSON and update both fields
    update_fields(json_data)

    # Return the modified JSON as a string, formatted with indentation
    updated_json = json.dumps(json_data, indent=4)

    return updated_json


def create_data_pipeline(item_name, item_type, access_token, artifact_path, target_folder, workspace_id, workspace_name, connections_data, eventhouse_dict, spn_access_token, guids, excluded_files={".platform"}):
    """
    Sends a request to the Create Item API to create or update a data pipeline in the target workspace.

    Parameters:
    item_name (str): The name of the item to deploy or redeploy.
    item_type (str): The type of the item (e.g., Notebook, DataPipeline).
    access_token (str): The authentication token for accessing the data pipeline API.
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path containing the items.
    workspace_id (str): The ID of the workspace where the item should be deployed or redeployed.
    connections_data (dict): A dictionary of all connection names and types.
    excluded_files (set): A set of file names to exclude from the publish process.

    Returns:
        None
    """

    try:
        start_time=datetime.now(timezone.utc)
        # Retrieve the repository items
        repository_items = repository_items_list(artifact_path, target_folder, workspace_id, access_token)

        # Retrieve item details
        item_path = repository_items[item_type][item_name]["path"]
        item_guid = repository_items[item_type][item_name]["guid"]
        item_description = repository_items[item_type][item_name]["description"]

        # Prepare metadata for the item
        metadata_body = {
            "displayName": item_name,
            "type": item_type,
            "description": item_description
        }

        item_payload = []
        kql_database_id = None
        endpoint = None

        # Get the directory part of the path (the parent directory of the file)
        parent_directory = os.path.dirname(item_path)

        for root, _, files in os.walk(parent_directory):
            for file in files:
                full_path = os.path.join(root, file)
                if file not in excluded_files:
                    try:
                        # Read the raw content of the file
                        with open(full_path, "r", encoding="utf-8") as f:
                            raw_file = f.read()

                        # Replace logical IDs with deployed GUIDs
                        replaced_raw_file = replace_logical_ids(raw_file, artifact_path, target_folder, workspace_id, access_token)

                        if eventhouse_dict:
                            eventhouse_name = list(eventhouse_dict.keys())[0]
                        
                            item_data = list_workspace_all_items(workspace_id, spn_access_token)
                            
                            kql_database_id = next(
                                (item["id"] for item in item_data if item["type"] == "KQLDatabase" and item["displayName"] == eventhouse_name),
                                None
                            )

                            
                            endpoint = get_kusto_uri(workspace_id, eventhouse_name, access_token)
                        updated_raw_file = update_connection_and_workspace_id(replaced_raw_file, connections_data, access_token, workspace_id, kql_database_id=kql_database_id, endpoint=endpoint)

                        byte_file = updated_raw_file.encode("utf-8")
                        payload = base64.b64encode(byte_file).decode("utf-8")

                        item_payload.append({
                            "path": full_path,
                            "payload": payload,
                            "payloadType": "InlineBase64"
                        })

                    except Exception as e:
                        raise Exception(f"Error processing file {full_path}: {str(e)}")

        definition_body = {
            "definition": {
                "parts": item_payload
            }
        }

        # Combine metadata and definition into the final body for publishing
        combined_body = {**metadata_body, **definition_body}
        
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }


        # Send the POST request to create the pipeline
        response = requests.post(url, headers=headers, json=combined_body)

        if response.status_code == 201: 
            response_data = response.json()
            guids.append({"artifact_type": "DataPipeline", "artifact_name": item_name, "artifact_location_guid": workspace_id, "artifact_location_name": workspace_name, "artifact_guid": response_data["id"]})
            return {"message": f"Pipeline '{item_name}' created successfully.", "data": response.json()}
        elif response.status_code == 202:
            response_data = handle_async_creation(response, headers)
            guids.append({"artifact_type": "DataPipeline", "artifact_name": item_name, "artifact_location_guid": workspace_id, "artifact_location_name": workspace_name, "artifact_guid": response_data["id"]})
            return {"message": f"Pipeline '{item_name}' created successfully.", "data": response.json()}
        else:
            raise Exception(f"Failed to create pipeline '{item_name}': {response.status_code} - {response.text}")

    except Exception as e:
        # Handle other unexpected errors
        error_message = str(e)
        raise Exception(f"Request error while creating pipeline: {error_message}")


def sort_datapipelines(unsorted_pipeline_dict, lookup_type, item_type, artifact_path, target_folder, workspace_id, access_token):
    """
    Returns a sorted list of data pipelines that should be published or unpublished, based on item dependencies.

    Parameters:
    unsorted_pipeline_dict (dict): A dictionary representation of the pipeline content file.
    lookup_type (str): Specifies whether to search for references in deployed files or repository files (Deployed or Repository).
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The folder within the artifact path where the items are located.
    workspace_id (str): The ID of the workspace where the pipelines should be created.
    access_token (str): The authentication token for accessing the data pipeline API.

    Returns:
    list: A sorted list of item names that should be published or unpublished, based on their dependencies.
    """
    
    try:
        # Step 1: Create a graph to manage dependencies
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        unpublish_items = []
        
        # Step 2: Build the graph and count the in-degrees
        for item_name, item_content_dict in unsorted_pipeline_dict.items():
            try:
                # In an unpublish case, keep track of items to get unpublished
                if (lookup_type.strip().lower() == "deployed"): 
                    unpublish_items.append(item_name)
                
                # Get referenced pipelines
                try:
                    referenced_pipelines = find_referenced_datapipelines(
                        item_type, item_content_dict, lookup_type, artifact_path, target_folder, workspace_id, access_token
                    )

                except Exception as e:
                    raise ValueError(f"Error finding referenced datapipelines for {item_name}: {str(e)}")
                
                # Build graph with dependencies
                for referenced_name in referenced_pipelines:
                    graph[referenced_name].append(item_name)
                    in_degree[item_name] += 1

                # Ensure every item has an entry in the in-degree map
                if item_name not in in_degree:
                    in_degree[item_name] = 0

            except Exception as e:
                raise ValueError(f"Error processing pipeline {item_name}: {str(e)}")

        # In an unpublish case, adjust in_degree to include entire dependency chain for each pipeline
        if lookup_type.strip().lower() == "deployed":
            try:
                for item_name in graph:
                    if item_name not in in_degree:
                        in_degree[item_name] = 0
                    for neighbor in graph[item_name]:
                        if neighbor not in in_degree:
                            in_degree[neighbor] += 1
            except Exception as e:
                raise ValueError(f"Error adjusting in-degrees in deployed case: {str(e)}")

        # Step 3: Perform a topological sort to determine the correct publish order
        zero_in_degree_queue = deque([item_name for item_name in in_degree if in_degree[item_name] == 0])
        sorted_items = []

        while zero_in_degree_queue:
            item_name = zero_in_degree_queue.popleft()
            sorted_items.append(item_name)

            for neighbor in graph[item_name]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    zero_in_degree_queue.append(neighbor)

        if len(sorted_items) != len(in_degree):
            raise ValueError("There is a cycle in the graph. Cannot determine a valid publish order.")

        # Remove items not present in unpublish list and invert order for deployed sort
        if lookup_type.strip().lower() == "deployed":
            sorted_items = [item_name for item_name in sorted_items if item_name in unpublish_items]
            sorted_items = sorted_items[::-1]

        return sorted_items
    
    except Exception as e:
        raise RuntimeError(f"Error in sorting datapipelines: {str(e)}")


def get_lakehouse_id(spn_access_token, workspace_id, lakehouse_name):
    """
    Fetches all lakehouses in the given Fabric workspace, finds the one whose displayName
    exactly equals `lakehouse_name`, and returns its GUID.

    Raises:
        RuntimeError: if the API call fails or if the named lakehouse is not found.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    headers = {
        "Authorization": f"Bearer {spn_access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(
            f"Failed to list lakehouses (HTTP {response.status_code}): {response.text}"
        ) from e

    data = response.json()
    # Assuming the JSON is { "value": [ { "id": "...", "displayName": "..." }, … ] }
    for lh in data.get("value", []):
        if lh.get("displayName") == lakehouse_name:
            return lh["id"]

    # not found
    raise RuntimeError(f"Lakehouse named '{lakehouse_name}' not found in workspace {workspace_id}")


def deploy_lakehouse(
    artifact_path,
    target_folder,
    spn_access_token,
    workspace_id,
):
    """
    Deploys lakehouses by filtering lakehouses from the specified artifact folder and creating them in the target workspace.

    Parameters:
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path to process.
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the lakehouses should be created.

    Returns:
    dict: A dictionary containing lakehouse display names and their corresponding IDs.

    Raises:
    Exception: If a lakehouse cannot be processed successfully, an exception is raised.
    """

    # Retrieve lakehouses and platform paths
    lakehouses, lh_platforms = filter_lakehouses(artifact_path, target_folder)

    # Initialize a dictionary to store the lakehouse IDs and display names
    lakehouse_dict = {}

    for idx, (lakehouse_path, lh_platform_path) in enumerate(zip(lakehouses, lh_platforms), start=1):
        try:
            # Read lakehouse metadata content
            with open(lakehouse_path, "r") as file:
                lakehouse_content = file.read()
            with open(lh_platform_path, "r") as file:
                platform_content = file.read()

            # Parse the JSON string
            platform_content_json = json.loads(platform_content)

            # Extract lakehouse name from the platform file
            lakehouse_name = platform_content_json["metadata"]["displayName"]
            item_type = platform_content_json["metadata"]["type"]

            # Create lakehouse in the workspace using the API
            response_data = create_lakehouse(
                spn_access_token,
                workspace_id,
                lakehouse_name
            )

            # Check if the response is valid and contains 'displayName' and 'id'
            if response_data and "displayName" in response_data and "id" in response_data:
                lakehouse_dict[response_data["displayName"]] = response_data["id"]
            else:
                error_msg = f"Invalid response for '{lakehouse_name}': {response_data}"
                raise Exception(error_msg)

        except Exception as e:
            error_context = f"Error processing lakehouse '{lakehouse_path}': {e}"
            raise Exception(error_context)

    return lakehouse_dict


def clean_deleted_item(existing_items, deployed_items, item_type, workspace_id, access_token):
    """
    Delete items from a workspace that no longer exist in the source notebook list.

    Parameters:
    - existing_items (dict): Dictionary of existing items fetched from the workspace API, keyed by ID.
    - deployed_items (list): List of notebook file paths from the local or source environment.
    - item_type (str): Type of item to be checked and potentially deleted (e.g., 'Notebook').
    - access_token (str): Bearer token used for authenticating API calls.

    Returns:
    - int: Number of items deleted from the workspace.

    Raises:
    - Exception: If the API call to delete an item fails.
    """

    
    # Get all notebook displayNames from existing_item
    existing_item_names = [
        item for item in existing_items.values() 
        if item["type"] == item_type
    ]

    # Extract notebook names from file paths (removing '.Notebook' and path parts)
    def normalize_path(path: str) -> str:
        return re.sub(r"[\\/]+", "/", path)

    item_from_path = [
        normalize_path(path).split("/")[-2].split(f".{item_type}")[0]
        for path in deployed_items
    ]
    
    items_to_delete = [
        item for item in existing_item_names 
        if item["displayName"] not in item_from_path
    ]

    # Define API endpoint for item deletion (requires workspace_id to be defined externally)
    api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/"

    # Set authorization headers for the API call
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    i = 0

    # Iterate over the items to delete them one by one
    for item in items_to_delete:
        # Rate limiting: pause after every 30 deletions
        if i % 30 == 0:
            time.sleep(55)
        i += 1

        item_name = item.get("displayName")
        item_id = item.get("id")

        response = requests.delete(f"{api_url}{item_id}", headers=headers)

        if response.status_code != 200:
            raise Exception(f"An error occurred while deleting item '{item_name}': {response.text}")


def deploy_notebooks(artifact_path, target_folder, lakehouse_dict, spn_access_token, workspace_id, workspace_name, existing_items, guids):
    """
    Deploys notebooks by filtering notebooks from the specified artifact folder, updating them with lakehouse information,
    and creating them in the target workspace.

    Parameters:
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path to process.
    lakehouse_dict (dict): A dictionary containing lakehouse IDs used to update the notebook content.
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the notebooks should be created.
    existing_items (dict) : A dictionary containing all the existing items of the workspace

    Returns:
    None
    """

    notebooks, platforms = filter_notebooks(artifact_path, target_folder)

    clean_deleted_item(existing_items, notebooks, "Notebook", workspace_id, spn_access_token)

    i = 0
    for notebook_path, platform_path in zip(notebooks, platforms):
        if i%30 == 0:
            time.sleep(55)
        i = i + 1
        try:
            # Read notebook content
            with open(notebook_path, "r") as file:
                notebook_content = file.read()
            with open(platform_path, "r") as file:
                platform_content = file.read()

            if "Data_Ingestion" in target_folder:
                lakehouse_dict = {k: v for k, v in lakehouse_dict.items() if "Bronze" in k}

            elif "Data_Non_Security" in target_folder:
                deployment_env = os.getenv("deployment_env")
                # Define paths to the configurations in ADO
                config_base_path = f"Configuration/{deployment_env}"
                deployment_profile_path = f"{config_base_path}/DEPLOYMENT_PROFILE.csv"
                deployment_profile_df = pd.read_csv(deployment_profile_path)

                bronze_workspace_details = deployment_profile_df["workspace_prefix"].str.contains(r"-bronze", na=False)
                bronze_df = deployment_profile_df[bronze_workspace_details]
                bronze_workspace_name = bronze_df["workspace_prefix"].iloc[0]

                # Check if workspace exists using API
                workspace_details = does_workspace_exists_by_name(bronze_workspace_name, spn_access_token)
                bronze_workspace_id = workspace_details["id"] if workspace_details else None
                
                bronze_lakehouse_guid = get_lakehouse_id(spn_access_token, bronze_workspace_id, "Bronze")
                
                # Overwrite lakehouse_dict so it contains only the Bronze‑bronze lakehouse
                lakehouse_dict = {"Bronze": bronze_lakehouse_guid}            
            
                
            # Update notebook content with lakehouse information
            new_notebook_content = update_notebook_content(notebook_content, lakehouse_dict, workspace_id, target_folder)             
           
            # Parse the JSON string
            platform_content_json = json.loads(platform_content)
            
            # Extract notebook name from the platform file
            notebook_name = platform_content_json["metadata"]["displayName"]
            item_type = platform_content_json["metadata"]["type"]
         
            # Create the notebook in the target workspace
            create_notebook(spn_access_token, workspace_id, workspace_name, notebook_name, new_notebook_content, notebook_path, platform_content, platform_path, existing_items, guids)
        
        except Exception as e:
            raise Exception(f"Error processing notebook '{notebook_path}': {str(e)}")


def get_unsorted_pipeline_dict(artifact_path, target_folder):
    """
    Constructs a dictionary of unsorted pipelines and extracts the item type from the specified artifact folder and target subfolder.
    
    Parameters:
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path to process.
    
    Returns:
    tuple: A tuple containing:
           - unsorted_pipeline_dict (dict): A dictionary of pipeline names as keys and their corresponding content as values.
           - item_type (str): The item type of the pipelines (extracted from the platform file).
    """
    
    pipelines, platforms = filter_pipelines(artifact_path, target_folder)

    # Initialize variables
    unsorted_pipeline_dict = {}
    item_type = None

    for pipeline_path, platform_path in zip(pipelines, platforms):
        try:
            # Open and read the pipeline content (with UTF-8 encoding)
            with open(pipeline_path, "r", encoding="utf-8") as file:
                pipeline_content = file.read()  # Read with UTF-8 encoding
            with open(platform_path, "r") as file:
                platform_content_json = json.load(file) 

            # Parse the raw JSON content
            item_content_dict = json.loads(pipeline_content)
            
            # Extract pipeline name from the platform file
            pipeline_name = platform_content_json["metadata"]["displayName"]
            item_type = platform_content_json["metadata"]["type"]

            # Add parsed data to the unsorted pipeline dict
            unsorted_pipeline_dict[pipeline_name] = item_content_dict        
        except Exception as e:
            raise Exception(f"Error processing pipeline '{pipeline_path}': {str(e)}")

    return unsorted_pipeline_dict, item_type


def deploy_pipelines(connections_data, artifact_path, target_folder, access_token, workspace_id, workspace_name, eventhouse_dict, spn_access_token, guids):
    """
    Deploys data pipelines by filtering pipeline files from the specified artifact folder and creating them in the target workspace.

    Parameters:
    connections_data (dict): A dictionary of all connection names and types.
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path to process.
    access_token (str): The authentication token for accessing the data pipeline API.
    workspace_id (str): The ID of the target workspace where the data pipeline should be created.
    guids (list): 

    Returns:
    None
    """

    # Get the unsorted pipeline dictionary and item_type by calling the new function
    unsorted_pipeline_dict, item_type = get_unsorted_pipeline_dict(artifact_path, target_folder)

    # Sort the data pipelines based on the publish order
    publish_order = sort_datapipelines(unsorted_pipeline_dict, "Repository", item_type, artifact_path, target_folder, workspace_id, access_token)
  
    for item_name in publish_order:
        if item_type is None:
            raise Exception("item_type is not defined properly.")
       
        create_data_pipeline(item_name=item_name, item_type=item_type, access_token=access_token, artifact_path=artifact_path, target_folder=target_folder, workspace_id=workspace_id, workspace_name=workspace_name, connections_data=connections_data, eventhouse_dict=eventhouse_dict, spn_access_token=spn_access_token, guids=guids)


def deploy_eventhouse(artifact_path, target_folder, spn_access_token, workspace_id):
    """
    Deploys eventhouses by filtering eventhouses from the specified artifact folder and creating them in the target workspace.

    Parameters:
    artifact_path (str): The root path of the artifact directory.
    target_folder (str): The subfolder within the artifact path to process.
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the eventhouses should be created.

    Returns:
    dict: A dictionary containing eventhouse display names and their corresponding IDs.

    Raises:
    Exception: If a eventhouse cannot be processed successfully, an exception is raised.
    """

    # Retrieve eventhouses and platform paths
    eventhouses, eh_platforms = filter_eventhouses(artifact_path, target_folder)
    # Initialize a dictionary to store the eventhouse IDs and display names
    eventhouse_dict = {}

    for eventhouse_path, eh_platform_path in zip(eventhouses, eh_platforms):
        try:
            # Read eventhouse metadata content
            with open(eventhouse_path, "r") as file:
                eventhouse_content = file.read()
            with open(eh_platform_path, "r") as file:
                platform_content_json = json.load(file) 
        
            # Extract eventhouse name from the platform file
            eventhouse_name = platform_content_json["metadata"]["displayName"]

            # Create eventhouse in the workspace using the API
            response_data = create_eventhouse(spn_access_token, workspace_id, eventhouse_name)

            # Check if the response is valid and contains 'displayName' and 'id'
            if response_data and "displayName" in response_data and "id" in response_data:
                eventhouse_dict[response_data["displayName"]] = response_data["id"]
            else:
                # Raise an exception if the response is invalid
                raise Exception(f"Invalid response data for eventhouse '{eventhouse_name}': {response_data}")

        except Exception as e:
            # Raise the error with additional context
            raise Exception(f"Error processing eventhouse '{eventhouse_path}': {str(e)}")
    return eventhouse_dict


def create_eventhouse(spn_access_token, workspace_id, eventhouse_name):
    """
    Sends a request to the Create Item API to create a eventhouse in the target workspace.

    Parameters:
    spn_access_token (str): The authentication token for accessing the API.
    workspace_id (str): The ID of the target workspace where the eventhouse will be created.
    eventhouse_name (str): The prefix for the eventhouse name.

    Returns:
    dict: The response data from the API request, containing information about the created eventhouse.
    app_id (str): Application identifier associated with the deployment.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/eventhouses"
    
    headers = {
        "Authorization": f"Bearer {spn_access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "displayName": eventhouse_name,
        "type": "Eventhouse",
    }

    try:
        start_time=datetime.now(timezone.utc)
        # Send the POST request to create the eventhouse
        response = requests.post(url, headers=headers, json=payload)
        
        response_data = response.json()
        
        if response.status_code == 201: 
            return response_data
        elif response.status_code == 202:
            return response_data
        else:
            raise Exception(f"Failed to create eventhouse '{eventhouse_name}': {response.status_code} - {response.text}")
        
    except Exception as e:
        # Handle other unexpected errors
        error_message = f"Request error while creating eventhouse '{eventhouse_name}': {str(e)}"

        raise Exception(error_message)


def filter_eventhouses(artifact_path, target_folder):
    """
    Filters and returns the paths of 'EventhouseProperties.json' and platform files from the specified folder in the artifact.

    Parameters:
    artifact_path (str): The root path of the artifact.
    target_folder (str): The subfolder (relative to artifact_path) to process.

    Returns:
    tuple: A tuple containing two lists:
        - eventhouses (list): A list of paths to 'EventhouseProperties.json' files.
        - eh_platforms (list): A list of paths to '.platform' files within Eventhouse subdirectories.
    """
    target_folder_path = os.path.join(artifact_path, target_folder)
    eventhouses = []
    eh_platforms = []

    try:
        # Check if the target folder exists
        if not os.path.exists(target_folder_path):
            # Try alternative case variations
            alternative_paths = []
            base_path = os.path.dirname(target_folder_path)
            folder_name = os.path.basename(target_folder_path)
            
            if os.path.exists(base_path):
                for item in os.listdir(base_path):
                    if item.lower() == folder_name.lower() and os.path.isdir(os.path.join(base_path, item)):
                        alternative_paths.append(os.path.join(base_path, item))
            
            if alternative_paths:
                target_folder_path = alternative_paths[0]
                print(f"Warning: Target folder '{target_folder}' not found. Using '{target_folder_path}' instead.")
            else:
                print(f"Warning: Target folder '{target_folder}' does not exist in the artifact. No eventhouses to deploy.")
                return eventhouses, eh_platforms

        # Traverse the directory to find 'EventhouseProperties.json' and platform files
        for root, _, files in os.walk(target_folder_path):
            # Check if the root contains '.Eventhouse' before proceeding to loop through the files
            if ".Eventhouse" in root:
                for file in files:
                    if file == "EventhouseProperties.json":
                        eventhouses.append(os.path.join(root, file))
                    elif file == ".platform":
                        eh_platforms.append(os.path.join(root, file))

    except Exception as e:
        print(f"Warning: An error occurred while filtering eventhouses: {str(e)}")
        return [], []

    return eventhouses, eh_platforms


def deploy_custom_environment(workspace_id, spn_access_token):
    """
    Deploys custom environment to the workspace.
    
    Parameters:
    workspace_id (str): The ID of the target workspace.
    spn_access_token (str): The authentication token for accessing the API.
    
    Returns:
    None
    """
    # This is a placeholder function - implement based on your requirements
    print("Custom environment deployment function called")


def deploy_artifacts(transformation_layer, connections_data, artifact_path, target_folder, spn_access_token, workspace_id, workspace_name, is_deployment, items={}):
    """
    Orchestrates the deployment of lakehouses, notebooks, and data pipelines to the target workspace.

    This function handles the deployment of various artifacts such as lakehouses, notebooks, and data pipelines
    to a specified Microsoft Fabric workspace. It uses provided authentication credentials, workspace identifiers,
    and optional deployment items to manage the process.

    Parameters:
    transformation_layer (str): The layer of the data pipeline or transformation (e.g., bronze, silver, gold).
    connections_data (dict): A dictionary containing connection metadata, such as names and types.
    artifact_path (str): The base directory path where all deployable artifacts are stored.
    target_folder (str): The specific subfolder under the artifact path containing artifacts for deployment.
    spn_access_token (str): The service principal token used for authenticating API calls to the workspace.
    workspace_id (str): The unique identifier of the target workspace.
    workspace_name (str): The name of the target workspace.
    is_deployment (bool): A flag indicating whether to execute a deployment (True) or perform a dry run/logging (False).
    items (dict, optional): A dictionary specifying individual items to deploy; defaults to an empty dict if not provided.

    Returns:
    None
    """
    
    try:
        existing_items = {f"{item['displayName']}.{item['type']}": item for item in items}
        guids = [{"artifact_type": "Workspace", "artifact_name": workspace_name, "artifact_location_guid": None, "artifact_location_name": None, "artifact_guid": workspace_id}]
        # Deploy Lakehouse
        try:
            if is_deployment:
                lakehouse_dict = deploy_lakehouse(artifact_path, target_folder, spn_access_token, workspace_id)
            else:
                lakehouse_dict = {item["displayName"]: item["id"] for item in items if item["type"].strip().lower() == "lakehouse"}
        except Exception:
            # Capture the traceback and raise a new exception with it
            error_message = f"Error during lakehouse deployment."
            raise Exception(error_message)  # Re-raise with the custom message including traceback

        # Deploy Eventhouse
        eventhouse_dict = {}
        try:
            if is_deployment:
                eventhouse_dict = deploy_eventhouse(artifact_path, target_folder, spn_access_token, workspace_id)
            else:
                eventhouse_dict = {item["displayName"]: item["id"] for item in items if item["type"].strip().lower() == "eventhouse"}
        except Exception:
            # Capture the traceback and raise a new exception with it
            error_message = f"Error during eventhouse deployment."
            raise Exception(error_message)  # Re-raise with the custom message including traceback

        # Deploy notebooks, pipelines, and environment
        try:
            deploy_notebooks(artifact_path, target_folder, lakehouse_dict, spn_access_token, workspace_id, workspace_name, existing_items, guids)
                
            deploy_pipelines(connections_data, artifact_path, target_folder, spn_access_token, workspace_id, workspace_name, eventhouse_dict, spn_access_token, guids)

            deploy_custom_environment(workspace_id, spn_access_token)
        except Exception:
            # Capture the traceback and raise a new exception with it
            error_message = f"Error during notebook/pipeline/environment deployment."
            raise Exception(error_message)  # Re-raise with the custom message including traceback
        
    except Exception:
        # Capture the traceback and raise a new exception with it
        error_message = f"Unhandled exception in deploy_artifacts():\n{traceback.format_exc()}"
        raise Exception(error_message)  # Re-raise with the custom message including traceback