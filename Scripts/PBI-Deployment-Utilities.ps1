# PBI-Deployment-Utilities.ps1
# This script contains utility functions for Power BI deployment

function Deploy-Report {
    param(
        [Parameter(Mandatory=$true)]
        [string]$ReportFolder,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ReportName
    )
    
    try {
        Write-Host "Deploying report: $ReportName"
        
        # Find report.json file
        $reportJsonFile = Join-Path $ReportFolder "report.json"
        
        if (-not (Test-Path $reportJsonFile)) {
            throw "report.json file not found in report folder"
        }
        
        # Read report definition
        $reportDefinition = Get-Content $reportJsonFile -Raw
        Write-Host "Report definition loaded: $($reportDefinition.Length) characters"
        
        # Prepare deployment payload
        $deploymentPayload = @{
            "name" = $ReportName
            "definition" = @{
                "parts" = @(
                    @{
                        "path" = "report.json"
                        "payload" = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($reportDefinition))
                        "payloadType" = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10
        
        # Deploy report using Fabric API
        $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        
        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        try {
            $response = Invoke-RestMethod -Uri $deployUrl -Method Post -Body $deploymentPayload -Headers $headers
            Write-Host "✓ Report deployed successfully"
            return $true
        } catch {
            if ($_.Exception.Response.StatusCode -eq 409) {
                Write-Host "Report already exists, attempting update..."
                
                # Try to update existing report
                $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$ReportName"
                try {
                    $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Patch -Body $deploymentPayload -Headers $headers
                    Write-Host "✓ Report updated successfully"
                    return $true
                } catch {
                    Write-Warning "Failed to update report: $_"
                    return $false
                }
            } else {
                throw $_
            }
        }
        
    } catch {
        Write-Error "Failed to deploy report: $_"
        return $false
    }
}

function Get-WorkspaceIdByName {
    param(
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceName,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken
    )
    
    try {
        $headers = @{
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        $uri = "https://api.fabric.microsoft.com/v1/workspaces"
        $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
        
        $workspace = $response.value | Where-Object { $_.displayName -eq $WorkspaceName }
        
        if ($workspace) {
            return $workspace.id
        } else {
            throw "Workspace '$WorkspaceName' not found"
        }
    } catch {
        Write-Error "Failed to get workspace ID: $_"
        return $null
    }
}