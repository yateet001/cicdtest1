function Get-SPNToken {
    <#
    .SYNOPSIS
    Retrieves Azure AD token using Service Principal credentials.

    .PARAMETER TenantId
    Azure AD Tenant ID.

    .PARAMETER ClientId
    Service Principal Client ID.

    .PARAMETER ClientSecret
    Service Principal Client Secret.

    .OUTPUTS
    Returns OAuth2 access token as string.
    #>

    param (
        [Parameter(Mandatory = $true)]
        [string]$TenantId,

        [Parameter(Mandatory = $true)]
        [string]$ClientId,

        [Parameter(Mandatory = $true)]
        [string]$ClientSecret
    )

    try {
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $ClientId
            client_secret = $ClientSecret
            scope         = "https://api.fabric.microsoft.com/.default"
        }

        $response = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $body -ContentType "application/x-www-form-urlencoded"
        return $response.access_token
    }
    catch {
        throw "Error fetching SPN token: $_"
    }
}


function Connect-PowerBI-SPN {
    <#
    .SYNOPSIS
    Connects to Power BI Service using Service Principal authentication.

    .PARAMETER TenantId
    Azure AD Tenant ID for the Service Principal.

    .PARAMETER ClientId
    Client ID of the Service Principal.

    .PARAMETER ClientSecret
    Client Secret of the Service Principal.

    .RETURNS
    None. Sets global variables for reports and workspaces.
    #>
    param (
        [string] $TenantId,
        [string] $ClientId,
        [string] $ClientSecret
    )

    # Secure string for secret
    $secureClientSecret = ConvertTo-SecureString $ClientSecret -AsPlainText -Force
    $credential = New-Object System.Management.Automation.PSCredential ($ClientId, $secureClientSecret)

    try {
        Connect-PowerBIServiceAccount -ServicePrincipal -TenantId $TenantId -Credential $credential
    }
    catch {
        Write-Error "Failed to connect Power BI using Service Principal: $_"
        exit 1
    }

    # Set global variables for later use
    $global:allPowerBIReports = Get-PowerBIReport -Scope Organization
    $global:allPowerBIWorkspaces = Get-PowerBIWorkspace -All
}