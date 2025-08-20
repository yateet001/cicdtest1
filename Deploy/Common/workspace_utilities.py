import requests
import json
import time 
import os
from datetime import datetime, timezone

def does_workspace_exists_by_name(workspace_name, token):
    """
    Checks if a Power BI workspace with the given name exists in the user's organization.

    Parameters:
    - workspace_name (str): The name of the Power BI workspace to search for.
    - token (str): The access token used for authentication to the Fabric API.
    Returns:
    - dict: The information about the workspace if it exists, as returned by the Fabric API.
    - None: Returns None if the workspace does not exist or if the API request fails.
    """

    try:
        url = f"https://api.powerbi.com/v1.0/myorg/groups?$filter=name eq '{workspace_name}'"
        response = None
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            json_response_str = None
            try:
                json_response_str = str(response.json())
            except Exception as e:
                json_response_str = str(response)
            raise Exception(f"Checking workspace failed: " + json_response_str)

        if len(response.json()["value"]) > 0:
            return response.json()["value"][0]  # Return workspace info if it exists
        
        return None
        
    except Exception as e:
        raise e

def delete_workspace(workspace_id, access_token):
    """
    Deletes a workspace in Microsoft Fabric using the API.

    Parameters:
    - workspace_id (str): The ID of the workspace to delete.
    - access_token (str): The authentication token for API access.

    Returns:
    - bool: True if the workspace was successfully deleted, False otherwise.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()  # Raise an error for non-2xx responses
        return True
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error deleting workspace {workspace_id}: {e}")

def create_workspace(workspace_name, capacity_id, token):
    """
    Creates a new workspace in Microsoft Fabric using the provided workspace name and capacity ID.
    Supports both trial version (no capacity ID) and premium capacity scenarios.

    Parameters:
    - workspace_name (str): The name to assign to the new workspace.
    - capacity_id (str): The capacity ID under which the workspace should be created. 
                         Can be None, NaN, or empty for trial versions.
    - token (str): The access token for authentication to the Fabric API.

    Returns:
    - str: The ID of the created workspace if successful.
    - None: Returns None if the workspace creation fails (i.e., the API does not return a 201 status code).
    """

    try:
        start_time = datetime.now(timezone.utc)
        url = "https://api.fabric.microsoft.com/v1/workspaces"

        # ---- Normalize capacity_id safely ----
        if capacity_id is None:
            capacity_id_str = ""
        else:
            capacity_id_str = str(capacity_id).strip()

        # Detect trial mode (no valid capacity id)
        is_trial = (
            os.getenv('FABRIC_TRIAL_VERSION', 'false').lower() == 'true'
            or capacity_id_str == ""
            or capacity_id_str.lower() in ["nan", "none"]
            or capacity_id_str == "capacity_id_1234567890"
            or capacity_id_str == "00000000-0000-0000-0000-000000000000"
        )

        # ---- Payload ----
        if is_trial:
            print(f"Creating trial workspace: {workspace_name} (no capacity ID)")
            payload = {
                "displayName": workspace_name
            }
        else:
            print(f"Creating premium workspace: {workspace_name} with capacity: {capacity_id_str}")
            payload = {
                "displayName": workspace_name,
                "capacityId": capacity_id_str
            }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        print(f"Request URL: {url}")
        print(f"Request Payload: {json.dumps(payload, indent=2)}")
        
        # ---- API Call ----
        response = requests.post(url, headers=headers, json=payload)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Content: {response.text}")

        if response.status_code == 201:
            workspace_data = response.json()
            workspace_id = workspace_data["id"]
            workspace_name = workspace_data["displayName"]
            print(f"✓ Workspace created successfully: {workspace_name}")
            print(f"  Workspace ID: {workspace_id}")
            return workspace_id
            
        elif response.status_code == 409:
            print(f"Workspace '{workspace_name}' already exists, attempting to retrieve existing workspace...")
            existing_workspace = does_workspace_exists_by_name(workspace_name, token)
            if existing_workspace:
                workspace_id = existing_workspace["id"]
                print(f"✓ Using existing workspace: {workspace_name}")
                print(f"  Workspace ID: {workspace_id}")
                return workspace_id
            else:
                raise Exception("Workspace exists but could not retrieve details")
        else:
            try:
                error_message = str(response.json())
            except Exception:
                error_message = response.text
            
            print(f"✗ Failed to create workspace: {error_message}")
            raise Exception(f"Unable to create workspace: {error_message}")
        
    except Exception as e:
        error_message = str(e)
        print(f"✗ Error creating workspace: {error_message}")
        raise Exception(f"Error creating workspace: {error_message}")
        
def parse_user_info(user_info_str):
    """
    Parses and cleans the user info string into a list of dictionaries.
    
    Parameters:
    - user_info_str (str): The string of user details in JSON format, separated by "|".
    
    Returns:
    list: A list of user information dictionaries.
    """
    
    user_info_list = []  # Initialize an empty list to store user information dictionaries
    try:
        split_user_info_list = user_info_str.split("|")
        seen_configuration = set()

        # Split the input string by "|" and iterate over each user info segment
        for user_info in split_user_info_list:
            # Remove leading/trailing spaces and replace single quotes with double quotes for JSON parsing
            user_info = user_info.strip().replace("'", "\"")
            
            try:
                # Parse the cleaned string into a dictionary
                user_info_dict = json.loads(user_info)
                
                # Trim whitespace from each key and value in the dictionary
                user_info_dict = {key.strip(): value.strip() for key, value in user_info_dict.items()}
                
                # Extract required fields safely
                identifier = user_info_dict.get("identifier", "").strip().lower()
                principal_type = user_info_dict.get("principalType", "").strip().lower()
                access = user_info_dict.get("access", "").strip().lower()

                if not identifier or not principal_type or not access:
                    raise Exception(f"Missing required fields in user info: {user_info_dict}")

                user_tuple = (identifier, principal_type, access)

                if user_tuple not in seen_configuration:
                    seen_configuration.add(user_tuple)
                    # Append the cleaned dictionary to the list
                    user_info_list.append(user_info_dict)
            
            except Exception as e:
                # Raise an exception if parsing fails for a particular user info segment
                raise Exception(f"Error parsing user info: {user_info}: {str(e)}")
        
        return user_info_list

    except Exception as e:
        raise Exception(f"An error occurred in functions parse_user_info: {str(e)}")

def validate_no_duplicates(user_info_list):
    """
    Validates that no user appears twice with different roles.

    Parameters:
    - user_info_list (list): A list of user information dictionaries.

    Raises:
    - Exception: If a user is found more than once with different roles.

    Returns:
    - bool: True if validation is successful and no duplicates are found.
    """
    
    seen_identifiers = {}  # Dictionary to track users and their assigned roles

    try:
        for user_info in user_info_list:
            identifier = user_info.get("identifier", "").strip().lower()
            role = user_info.get("access", "").strip().lower()

            if not identifier or not role:
                raise ValueError("Both 'identifier' and 'access' fields must be non-empty.")

            if identifier in seen_identifiers and seen_identifiers[identifier] != role:
                raise Exception(
                    f"User '{identifier}' has been specified more than once with different roles: "
                    f"'{seen_identifiers[identifier]}' and '{role}'."
                )

            seen_identifiers[identifier] = role  # Store identifier-role pair

        return True  # Return True if validation passes
    except Exception as e:
        raise Exception(f"An error ocurred while validating the configuration for the users: {str(e)}")

def list_current_workspace_users(workspace_id, token):
    """
    Lists the current users in the specified Power BI workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token.
    
    Returns:
    list: A list of user identifiers currently added to the workspace.
    
    Raises:
    Exception: If the API request to fetch users fails.
    """

    try:
        # Power BI API endpoint for listing workspace users
        api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.get(api_url, headers=headers)

        # Raise an error if the API request fails
        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch current users of workspace {workspace_id}: {response.status_code} {response.text}"
            )

        # Extract user identifiers and convert them to lowercase for consistency
        current_users = {user["identifier"].lower(): user["groupUserAccessRight"].lower() for user in response.json().get("value", [])}

        return current_users  # Return the list of user identifiers

    except Exception as e:
        raise Exception(f"And error occurred while getting current workspace users: {str(e)}")

def prepare_users_to_add(user_info_list, current_users):
    """
    Prepares a list of users to add to the workspace.
    
    Parameters:
    - user_info_list (list): A list of user information dictionaries.
    - current_users (dict): A list of user identifiers already in the workspace.
    
    Returns:
    list: A list of user dictionaries to add to the workspace.
    """
    try:
        users_to_add = []  # Initialize an empty list to collect users who need to be added
        current_user_names = current_users.keys()
        users_to_update_access = []

        # Iterate through each user's information in the provided list
        for user_info in user_info_list:
            # Retrieve and normalize the user's identifier for comparison
            identifier = user_info.get("identifier", "").strip().lower()
            access = user_info.get("access", "").strip().lower()

            # Check if the user is not already in the list of current users
            if identifier not in current_user_names:
                users_to_add.append(user_info)  # Add the user info to the list for adding

            else:
                if current_users[identifier].lower() != access:
                    users_to_update_access.append(user_info)

                current_users[identifier] = None

        return users_to_add, users_to_update_access, current_users  # Return the list of users to be added       
    except Exception as e:
        raise Exception(f"An error occurred in function prepare users to add: {str(e)}")

def send_request_user_to_workspace(workspace_id, token, user):
    """
    Adds a user to the Power BI workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token or client instance.
    - user (dict): A dictionary containing user details to add to the workspace.
    
    Returns:
    response: The response object from the API request.
    
    Raises:
    Exception: If the API call fails or an error occurs.
    """
    try:
        # Power BI API endpoint to add users to the workspace
        api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"

        # Construct the payload with the user information
        payload = {
            "principalType": user.get("principalType"),  # Type of principal (e.g., User, Group)
            "identifier": user.get("identifier"),  # User identifier (e.g., email or user ID)
            "groupUserAccessRight": user.get("access")  # Access rights (e.g., Admin, Member)
        }

        # Make API request based on whether the authorization is a token or client
        headers = {
            "Authorization": f"Bearer {token}",  # Use the provided token for authentication
            "Content-Type": "application/json"  # Specify content type as JSON
        }

        # Send POST request with headers and payload
        response = requests.post(api_url, headers=headers, json=payload)
        return response  # Return the response object
    except Exception as e:
        raise Exception(f"An error occurred in function send_request_user_to_workspace: {str(e)}")
        
def add_users(workspace_id, token, users_to_add):
    """
    Batches users and sends them to the Power BI workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token.
    - users_to_add (list): A list of users to add to the workspace.

    Raises:
    Exception: If an error occurs while adding users to the workspace.
    """
    
    try:
        # Loop through each user in the list of users to add
        for user in users_to_add:
            # Send a request to add the user to the workspace
            response = send_request_user_to_workspace(workspace_id, token, user)
            
            # Raise an exception if the response status code is not 200 (success)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to add user: {response.status_code}, {response.text}"
                )
    
    except Exception as e:
        # Catch and raise any exceptions that occur during the process
        raise Exception(f"Error occurred while adding users to workspace: {str(e)}")

def update_user_access(workspace_id, token, users_to_update):
    """
    Adds a user to the Power BI workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token.
    - user (list): A dictionary containing user details to update access on the workspace.
        
    Returns:
    response: The response object from the API request.
    
    Raises:
    Exception: If the API call fails or an error occurs.
    """
    
    try:
        # Loop through each user in the list of users to add
        for user in users_to_update:
            # Send a request to update access of the user to the workspace
            api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"

            # Construct the payload with the user information
            payload = {
                "principalType": user.get("principalType"),  # Type of principal (e.g., User, Group)
                "identifier": user.get("identifier"),  # User identifier (e.g., email or user ID)
                "groupUserAccessRight": user.get("access")  # Access rights (e.g., Admin, Member)
            }

            # Make API request based on whether the authorization is a token or client
            headers = {
                "Authorization": f"Bearer {token}",  # Use the provided token for authentication
                "Content-Type": "application/json"  # Specify content type as JSON
            }

            # Send POST request with headers and payload
            response = requests.put(api_url, headers=headers, json=payload)

            # Raise an exception if the response status code is not 200 (success)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to update user access: {response.status_code}, {response.text}"
                )
    
    except Exception as e:
        # Catch and raise any exceptions that occur during the process
        raise Exception(f"Error occurred while updating access of users to workspace: {str(e)}")

def remove_users(workspace_id, token, users_to_remove):
    """
    Batches users and sends them to the Power BI workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token.
    - users_to_remove (dict): A list of users to remove from workspace.
    
    Raises:
    Exception: If an error occurs while adding users to the workspace.
    """

    try:
        api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"

        for user, access in users_to_remove.items():
            if access is not None:
                headers = {
                    "Authorization": f"Bearer {token}",  # Use the provided token for authentication
                    "Content-Type": "application/json"  # Specify content type as JSON
                }
                # Send POST request with headers and payload
                response = requests.delete(f"{api_url}/{user}", headers=headers)

                # Raise an exception if the response status code is not 200 (success)
                if response.status_code != 200:
                    raise Exception(
                        f"Failed to update user access: {response.status_code}, {response.text}"
                    )
    
    except Exception as e:
        raise Exception(f"An error occurred while removing users from the workspace: {str(e)}")

def add_security_group_to_workspace(workspace_id, workspace_name, token, user_info_str):
    """
    Adds security groups to a Power BI workspace. Handles adding multiple users in batches with error handling.
    
    Parameters:
    - workspace_id (str): The ID of the Power BI workspace.
    - token (str): The authorization token to access the Power BI API.
    - user_info_str (str): A string of user details in JSON format, separated by "|".

    """
    try:
        start_time = datetime.now(timezone.utc)
        # Parse and clean the user info string into a list of dictionaries
        user_info_list = parse_user_info(user_info_str)

        # Validate no duplicates with different roles
        validate_no_duplicates(user_info_list)

        # List the current users in the workspace using API
        current_users = list_current_workspace_users(workspace_id, token)

        # Prepare the list of users to add
        users_to_add, update_access, users_to_remove = prepare_users_to_add(user_info_list, current_users)

        # Send request to add user through the API
        add_users(workspace_id, token, users_to_add)
        update_user_access(workspace_id, token, update_access)
        remove_users(workspace_id, token, users_to_remove)

    except Exception as e:
        raise Exception(f"Error occurred while adding security group to workspace: {str(e)}")