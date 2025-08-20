import requests
from azure.identity import UsernamePasswordCredential

def get_spn_access_token(tenant_id, client_id, client_secret):
    """
    Retrieve an access token using a service principal.

    parameters:
    - tenant_id: Tenant of the Azure Active Directory tenant
    - client_id: The client ID of the service principal
    - client_secret: The client secret of the service principal

    returns:
    - str: The access token (Bearer token)
    """

    # Set the token endpoint URL for Azure AD
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    # Define the payload with necessary parameters for authentication
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default",
    }

    # Make a POST request to get the token
    response = requests.post(url, data=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response to extract the access token
        token_data = response.json()
        return token_data["access_token"]
    else:
        # Raise an exception if there was an error in the request
        raise Exception(f"Failed to get access token: {response.status_code}, {response.text}")


def get_upn_access_token(upn_client_id, upn_user_id, upn_password):
    """
    Retrieve an access token using UPN (username/password) credentials.

    parameters:
    - upn_client_id: The client ID of the application
    - upn_user_id: The UPN (username) of the user
    - upn_password: The password of the user

    returns:
    - str: The access token (Bearer token)
    """

    try:
        # Create the credential object
        credential = UsernamePasswordCredential(
            client_id=upn_client_id,
            username=upn_user_id,
            password=upn_password
        )
        
        # Get the access token for Power BI API (can be adjusted for different APIs)
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        
        # Return the access token
        return token
    
    except Exception as e:
        # Raise an exception with the error message if there was an error in the request
        raise Exception(f"Failed to get access token: {str(e)}")
