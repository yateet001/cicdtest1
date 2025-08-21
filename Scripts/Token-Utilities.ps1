# Token-Utilities.ps1
# This script contains utility functions for token management

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
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret
    )
    
    try {
        Write-Host "Acquiring access token for Fabric API..."
        
        # Use Fabric API scope for PBIP deployment
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $ClientId
            client_secret = $ClientSecret
            scope         = "https://api.fabric.microsoft.com/.default"
        }
        
        $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Method Post -Body $body
        $accessToken = $tokenResponse.access_token
        
        Write-Host "✓ Successfully acquired Fabric API access token"
        return $accessToken
    }
    catch {
        Write-Error "Failed to acquire access token for Fabric API: $_"
        
        # Fallback to Power BI API scope
        try {
            Write-Host "Trying Power BI API scope as fallback..."
            
            $body = @{
                grant_type    = "client_credentials"
                client_id     = $ClientId
                client_secret = $ClientSecret
                resource      = "https://analysis.windows.net/powerbi/api"
            }
            
            $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$TenantId/oauth2/token" -Method Post -Body $body
            $accessToken = $tokenResponse.access_token
            
            Write-Host "✓ Successfully acquired Power BI API access token as fallback"
            return $accessToken
        }
        catch {
            Write-Error "Failed to acquire Power BI API access token: $_"
            throw "Could not acquire any access token"
        }
    }
}

function Get-AccessTokenFromConfig {
    param(
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret
    )
    
    return Get-SPNToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
}