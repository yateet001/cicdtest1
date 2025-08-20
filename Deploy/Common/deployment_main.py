import os 
import json
import requests
import time 
from datetime import datetime, timezone
from workspace_utilities import *
from token_utilities import *
from workspace_item_utilities import *
import pandas as pd

spn = os.getenv("spn")
deployment_env = os.getenv("deployment_env")
environment_type = os.getenv("environment_type")
artifact_path = os.getenv("artifact_path")
build_number = os.getenv("build_number")
connections_json = os.getenv("connections") 

# Parse the JSON string
connections_data = json.loads(connections_json)

trimmed_lower_deployment_env = deployment_env.lower().strip()
trimmed_lower_environment_type = environment_type.lower().strip()

# Define paths to the configurations in ADO
config_base_path = f"Configuration/{deployment_env}"
deployment_profile_path = f"{config_base_path}/DEPLOYMENT_PROFILE.csv"
configuration_files_list = ["DEPLOYMENT_PROFILE.csv", "IN_TAKE_CONFIG.csv"]

def orchestrator(tenant_id, client_id, client_secret, connections_data):
    """
    Orchestrates the deployment of networks.

    Parameters:
    - tenant_id (str): The Azure Active Directory tenant ID used for authentication.
    - client_id (str): The client ID (application ID) used for authentication with Azure.
    - client_secret (str): The client secret associated with the Azure application.
    - connections_data (dict): A dictionary of all connection names and types.

    Raises:
    Exception: If any error occurs during onboarding of networks.
    """

    error_message = ""

    try:
        # Read deployment and capacity configuration files
        all_deployment_profile_df = pd.read_csv(deployment_profile_path)

        # Filter the deployment profiles for the environments and networks to be onboarded
        deployment_operation_ws_details_df = all_deployment_profile_df[
            (all_deployment_profile_df["to_be_onboarded"]) &
            (all_deployment_profile_df["deployment_env"].str.strip().str.lower() == trimmed_lower_deployment_env) &
            (all_deployment_profile_df["environment_type"].str.strip().str.lower() == trimmed_lower_environment_type) &
            (all_deployment_profile_df["transformation_layer"].str.strip().str.lower() == "operations")
        ]

        # Ensure there is at least one matching row
        if deployment_operation_ws_details_df.empty:
            raise ValueError("No matching deployment profile found.")

        # Extract the single record correctly
        row = deployment_operation_ws_details_df.iloc[0]
        # Use workspaceName from environment variable if provided, else from CSV
        workspace_name = os.getenv("workspaceName", row["workspace_prefix"])
        # If using trial workspace, ignore capacity_id
        if workspace_name in ["VISACICDDev", "VISACICDQA"]:
            capacity_id = None
        else:
            capacity_id = row.get("capacity_id", None)
        
        # Fix: Use the exact case from CSV but ensure it matches the artifact structure
        transformation_layer = row["transformation_layer"].strip()
        # Since the artifact structure uses "Operations" with capital O, ensure proper casing
        if transformation_layer.lower() == "operations":
            transformation_layer = "Operations"
            
        workspace_users = row["workspace_default_groups"]
        deployment_code = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        spn_access_token = get_spn_access_token(tenant_id, client_id, client_secret)

        # Check if workspace exists using API
        operations_workspace_details = does_workspace_exists_by_name(workspace_name, spn_access_token)
        workspace_id = operations_workspace_details["id"] if operations_workspace_details else None

        # Check if the workspace does not already exist
        if workspace_id is None:
            try:
                # Create a new workspace (capacity_id will be None for trial)
                workspace_id = create_workspace(workspace_name, capacity_id, spn_access_token)

                # Add security group/users to the new workspace
                are_user_added = add_security_group_to_workspace(
                    workspace_id, workspace_name, spn_access_token, workspace_users
                )

                # Mark the deployment as full
                is_deployment = True

                # Deploy artifacts to the newly created workspace
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    is_deployment, items={}
                )

            except Exception as e:
                # If creation fails and workspace was created, delete it for cleanup
                if workspace_id:
                    delete_workspace(workspace_id, spn_access_token)
                    workspace_id = None
                # Re-raise the exception after cleanup
                raise e

        else:
            # If workspace already exists, update it incrementally
            try:
                is_deployment = False

                # Ensure the security group/users are still added to the workspace
                are_user_added = add_security_group_to_workspace(
                    workspace_id, workspace_name, spn_access_token, workspace_users
                )

                # Fetch the list of existing items in the workspace
                items = list_workspace_all_items(workspace_id, spn_access_token)

                # Delete outdated or obsolete items before deploying new ones
                are_items_deleted = delete_old_items(
                    workspace_id, items, artifact_path, "ARM/" + transformation_layer, spn_access_token
                )

                # Wait for some time before redeploying to ensure deletions are processed
                time.sleep(450)

                # Redeploy updated artifacts to the existing workspace
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    is_deployment, items=items
                )

            except Exception as exc:
                error_message = error_message + str(exc)

        if error_message:
            raise Exception(error_message)
    except Exception as e:
        raise e # Re-raise for debugging

if __name__ == "__main__":
    try:
        # Preprocess the secret values to replace single quotes with double quotes
        spn_secret_json_value = spn.replace("'", '"')
        
        # Attempt to parse the SPN secret JSON string into a Python dictionary
        key_vault_spn_secrets = json.loads(spn_secret_json_value)

        # Extract individual values from the parsed SPN secrets dictionary
        tenant_id = key_vault_spn_secrets["tenant_id"]
        client_id = key_vault_spn_secrets["client_id"]
        client_secret = key_vault_spn_secrets["client_secret"]

        # Call the orchestrator function with the extracted values as arguments
        orchestrator(tenant_id, client_id, client_secret, connections_data)
    
    except Exception as e:
        # If an error occurs during the execution, raise the exception
        raise e