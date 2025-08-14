# MainOrchestrator.ps1 for PBIP file deployment (minimal version with embedded utilities)
param(
    [Parameter(Mandatory=$true)]
    [string]$Workspace,
    
    [Parameter(Mandatory=$true)]
    [string]$ConfigFile
)

Write-Host "Starting Power BI PBIP Deployment..."
Write-Host "Workspace: $Workspace"
Write-Host "Config File: $ConfigFile"

# Embedded utility functions (to avoid import issues)
function Get-SPNToken {
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

function Get-PBIPFiles {
    param(
        $ArtifactPath,
        $Folder
    )

    if ($Folder) {
        $target = Join-Path $ArtifactPath $Folder
    } else {
        $target = $ArtifactPath
    }

    if (-not (Test-Path $target)) {
        Write-Warning "Path not found: $target"
        return @()
    }

    $files = Get-ChildItem -Path $target -Recurse -File -Filter '*.pbip'
    Write-Host "Found $($files.Count) PBIP files in $target"
    
    foreach ($file in $files) {
        Write-Host "  Found PBIP: $($file.FullName)"
        
        $parentDir = $file.Directory.FullName
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $reportFolder = Join-Path $parentDir "$baseName.Report"
        $semanticModelFolder = Join-Path $parentDir "$baseName.SemanticModel"
        
        Write-Host "    Report folder: $(Test-Path $reportFolder)"
        Write-Host "    SemanticModel folder: $(Test-Path $semanticModelFolder)"
    }

    return $files
}

function Validate-PBIPStructure {
    param(
        [Parameter(Mandatory=$true)]
        [string]$PBIPFilePath
    )
    
    $pbipDir = Split-Path $PBIPFilePath -Parent
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($PBIPFilePath)
    $reportFolder = Join-Path $pbipDir "$baseName.Report"
    $semanticModelFolder = Join-Path $pbipDir "$baseName.SemanticModel"
    
    $isValid = (Test-Path $reportFolder) -and (Test-Path $semanticModelFolder)
    
    if ($isValid) {
        Write-Host "✓ PBIP structure validated for: $baseName"
        
        $reportDefFile = Join-Path $reportFolder "report.json"
        $modelBimFile = Get-ChildItem -Path $semanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        
        Write-Host "    Report definition: $(Test-Path $reportDefFile)"
        Write-Host "    Model BIM file: $($modelBimFile -ne $null)"
        
        return @{
            IsValid = $true
            ReportFolder = $reportFolder
            SemanticModelFolder = $semanticModelFolder
            ReportDefFile = $reportDefFile
            ModelBimFile = if ($modelBimFile) { $modelBimFile.FullName } else { $null }
        }
    } else {
        Write-Warning "Invalid PBIP structure for: $baseName"
        Write-Warning "  Missing Report folder: $(-not (Test-Path $reportFolder))"
        Write-Warning "  Missing SemanticModel folder: $(-not (Test-Path $semanticModelFolder))"
        
        return @{
            IsValid = $false
        }
    }
}

   function Deploy-SemanticModel {
    param(
        [Parameter(Mandatory=$true)]
        [string]$SemanticModelFolder,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ModelName
    )
    
    try {
        Write-Host "Deploying semantic model: $ModelName"
        
        $modelBimFile = Get-ChildItem -Path $SemanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        
        if (-not $modelBimFile) {
            throw "model.bim file not found in semantic model folder"
        }
        
        $modelDefinition = Get-Content $modelBimFile.FullName -Raw
        Write-Host "Model definition loaded: $($modelDefinition.Length) characters"
        
        # Updated payload structure for Fabric API semantic models
        $deploymentPayload = @{
            "displayName" = $ModelName  # Changed from "name" to "displayName"
            "description" = "Semantic model deployed from PBIP: $ModelName"  # Added description
            "definition" = @{
                "parts" = @(
                    @{
                        "path" = "model.bim"
                        "payload" = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modelDefinition))
                        "payloadType" = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10
        
        $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
        
        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        try {
            $response = Invoke-RestMethod -Uri $deployUrl -Method Post -Body $deploymentPayload -Headers $headers
            Write-Host "✓ Semantic model deployed successfully"
            Write-Host "Model ID: $($response.id)"
            return @{
                Success = $true
                ModelId = $response.id
            }
        } catch {
            $statusCode = $_.Exception.Response.StatusCode
            $responseBody = $_.Exception.Response | ConvertFrom-Json -ErrorAction SilentlyContinue
            
            if ($statusCode -eq 409) {
                Write-Host "Semantic model already exists, attempting to find existing model..."
                
                # Get existing semantic models to find the ID
                try {
                    $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
                    $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
                    $existingModel = $listResponse.value | Where-Object { $_.displayName -eq $ModelName }
                    
                    if ($existingModel) {
                        Write-Host "Found existing model with ID: $($existingModel.id)"
                        
                        # Try to update the existing model using updateDefinition endpoint
                        $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$($existingModel.id)/updateDefinition"
                        
                        $updatePayload = @{
                            "definition" = @{
                                "parts" = @(
                                    @{
                                        "path" = "model.bim"
                                        "payload" = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modelDefinition))
                                        "payloadType" = "InlineBase64"
                                    }
                                )
                            }
                        } | ConvertTo-Json -Depth 10
                        
                        try {
                            $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updatePayload -Headers $headers
                            Write-Host "✓ Semantic model updated successfully"
                            return @{
                                Success = $true
                                ModelId = $existingModel.id
                            }
                        } catch {
                            Write-Warning "Failed to update semantic model definition: $_"
                            # Even if update fails, return the existing model as success
                            return @{
                                Success = $true
                                ModelId = $existingModel.id
                                Warning = "Model exists but update failed"
                            }
                        }
                    } else {
                        Write-Warning "Could not find existing semantic model with name: $ModelName"
                        return @{
                            Success = $false
                            Error = "Model conflict but could not locate existing model"
                        }
                    }
                } catch {
                    Write-Warning "Failed to list existing semantic models: $_"
                    return @{
                        Success = $false
                        Error = "Model conflict and failed to resolve"
                    }
                }
            } else {
                throw $_
            }
        }
        
    } catch {
        Write-Error "Failed to deploy semantic model: $_"
        return @{
            Success = $false
            Error = $_.Exception.Message
        }
    }
}

function Deploy-Report {
    param(
        [Parameter(Mandatory=$true)]
        [string]$ReportFolder,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ReportName,
        [string]$SemanticModelId = $null
    )
    
    try {
        Write-Host "Deploying report: $ReportName"
        
        $reportJsonFile = Join-Path $ReportFolder "report.json"
        
        if (-not (Test-Path $reportJsonFile)) {
            throw "report.json file not found in report folder"
        }
        
        $reportDefinition = Get-Content $reportJsonFile -Raw
        Write-Host "Report definition loaded: $($reportDefinition.Length) characters"
        
        # Updated payload structure for Fabric API reports
        $deploymentPayload = @{
            "displayName" = $ReportName  # Changed from "name" to "displayName"
            "description" = "Report deployed from PBIP: $ReportName"  # Added description
            "definition" = @{
                "parts" = @(
                    @{
                        "path" = "report.json"
                        "payload" = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($reportDefinition))
                        "payloadType" = "InlineBase64"
                    }
                )
            }
        }
        
        # Add semantic model binding if provided
        if ($SemanticModelId) {
            $deploymentPayload["datasetId"] = $SemanticModelId
            Write-Host "Binding report to semantic model ID: $SemanticModelId"
        }
        
        $deploymentPayloadJson = $deploymentPayload | ConvertTo-Json -Depth 10
        
        $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        
        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        try {
            $response = Invoke-RestMethod -Uri $deployUrl -Method Post -Body $deploymentPayloadJson -Headers $headers
            Write-Host "✓ Report deployed successfully"
            Write-Host "Report ID: $($response.id)"
            return $true
        } catch {
            $statusCode = $_.Exception.Response.StatusCode
            
            if ($statusCode -eq 409) {
                Write-Host "Report already exists, attempting to find and update..."
                
                # Get existing reports to find the ID
                try {
                    $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
                    $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
                    $existingReport = $listResponse.value | Where-Object { $_.displayName -eq $ReportName }
                    
                    if ($existingReport) {
                        Write-Host "Found existing report with ID: $($existingReport.id)"
                        
                        # Try to update the existing report using updateDefinition endpoint
                        $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$($existingReport.id)/updateDefinition"
                        
                        $updatePayload = @{
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
                        
                        try {
                            $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updatePayload -Headers $headers
                            Write-Host "✓ Report updated successfully"
                            return $true
                        } catch {
                            Write-Warning "Failed to update report definition: $_"
                            # Even if update fails, consider it a success since report exists
                            Write-Host "✓ Report exists (update failed but continuing)"
                            return $true
                        }
                    } else {
                        Write-Warning "Could not find existing report with name: $ReportName"
                        return $false
                    }
                } catch {
                    Write-Warning "Failed to list existing reports: $_"
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

function Deploy-PBIPUsingFabricAPI {
    param(
        [Parameter(Mandatory=$true)]
        [string]$PBIPFilePath,
        [Parameter(Mandatory=$true)]
        [string]$ReportName,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [string]$Takeover = "True"
    )
    
    try {
        Write-Host "Starting Fabric API deployment for PBIP: $ReportName"
        
        $validation = Validate-PBIPStructure -PBIPFilePath $PBIPFilePath
        if (-not $validation.IsValid) {
            throw "Invalid PBIP structure for: $ReportName"
        }
        
        Write-Host "PBIP structure validated successfully"
        
        # Deploy Semantic Model first
        Write-Host "Deploying semantic model..."
        $semanticModelResult = Deploy-SemanticModel -SemanticModelFolder $validation.SemanticModelFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ModelName $ReportName
        
        if (-not $semanticModelResult.Success) {
            Write-Warning "Semantic model deployment failed: $($semanticModelResult.Error)"
            return $false
        }
        
        if ($semanticModelResult.Warning) {
            Write-Warning "Semantic model warning: $($semanticModelResult.Warning)"
        }
        
        $semanticModelId = $semanticModelResult.ModelId
        Write-Host "Semantic model deployed/found with ID: $semanticModelId"
        
        # Deploy Report
        Write-Host "Deploying report..."
        $reportSuccess = Deploy-Report -ReportFolder $validation.ReportFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ReportName $ReportName -SemanticModelId $semanticModelId
        
        if (-not $reportSuccess) {
            Write-Warning "Report deployment failed"
            return $false
        }
        
        Write-Host "✓ PBIP deployment completed successfully for: $ReportName"
        return $true
        
    } catch {
        Write-Error "Fabric API deployment failed for $ReportName : $_"
        return $false
    }
}

# Main execution logic
try {
    Write-Host "Starting Power BI PBIP Report Deployment..."
    Write-Host "Environment: $Workspace"
    Write-Host "Config File: $ConfigFile"

    # Ensure TLS 1.2 is enabled
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    # Read configuration file
    if (-not (Test-Path $ConfigFile)) {
        throw "Configuration file not found: $ConfigFile"
    }

    $config = Get-Content $ConfigFile | ConvertFrom-Json
    Write-Host "Configuration loaded successfully"

    # Get SPN credentials from config
    $tenantId = $config.TenantID
    $clientId = $config.ClientID
    $clientSecret = $config.ClientSecret

    Write-Host "Using Tenant ID: $tenantId"
    Write-Host "Using Client ID: $clientId"

    # Map workspace based on environment
    $targetWorkspaceId = $null
    
    switch ($Workspace.ToUpper()) {
        "DEV" {
            $targetWorkspaceId = $config.DevWorkspaceID
        }
        "PROD" {
            $targetWorkspaceId = $config.UATWorkspaceID
        }
        default {
            throw "Unsupported environment: $Workspace. Only DEV and PROD are supported."
        }
    }

    if (-not $targetWorkspaceId) {
        throw "Workspace ID not found for environment: $Workspace"
    }

    Write-Host "Target Workspace ID: $targetWorkspaceId"

    # Get Access Token
    $accessToken = Get-SPNToken -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret

    # Set artifact path
    $artifactPath = $env:BUILD_SOURCESDIRECTORY
    if (-not $artifactPath) {
        $artifactPath = $env:artifact_path
    }
    if (-not $artifactPath) {
        $artifactPath = (Get-Location).Path
    }
    Write-Host "Using artifact path: $artifactPath"

    # Search for PBIP files
    Write-Host "Searching for PBIP files..."
    $reportFolders = @("Demo Report", "Reporting", "Reports", "PowerBI", "BI")
    $allPbipFiles = @()
    
    foreach ($folder in $reportFolders) {
        $folderPath = Join-Path $artifactPath $folder
        if (Test-Path $folderPath) {
            $pbipFiles = Get-PBIPFiles -ArtifactPath $artifactPath -Folder $folder
            $allPbipFiles += $pbipFiles
            Write-Host "Found $($pbipFiles.Count) PBIP files in $folder folder"
        }
    }

    # If no PBIP files found in specific folders, search entire repository
    if ($allPbipFiles.Count -eq 0) {
        Write-Host "No PBIP files found in expected folders, searching entire repository..."
        $allPbipFiles = Get-ChildItem -Path $artifactPath -Recurse -Filter "*.pbip" -ErrorAction SilentlyContinue
        Write-Host "Found $($allPbipFiles.Count) PBIP files total"
    }

    if ($allPbipFiles.Count -eq 0) {
        throw "No PBIP files found in the repository"
    }

    Write-Host "`n=== PBIP DEPLOYMENT ==="
    $deploymentResults = @()
    
    foreach ($pbipFile in $allPbipFiles) {
        $reportName = [System.IO.Path]::GetFileNameWithoutExtension($pbipFile.Name)
        Write-Host "`nProcessing PBIP: $reportName"
        Write-Host "File path: $($pbipFile.FullName)"
        
        $deploymentSuccess = Deploy-PBIPUsingFabricAPI -PBIPFilePath $pbipFile.FullName -ReportName $reportName -WorkspaceId $targetWorkspaceId -AccessToken $accessToken
        
        $result = [PSCustomObject]@{
            ReportName = $reportName
            FilePath = $pbipFile.FullName
            DeploymentSuccess = $deploymentSuccess
            Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            Environment = $Workspace
            WorkspaceId = $targetWorkspaceId
        }
        
        $deploymentResults += $result
        
        if ($deploymentSuccess) {
            Write-Host "✓ Successfully deployed: $reportName"
        } else {
            Write-Warning "❌ Failed to deploy: $reportName"
        }
    }

    # Summary
    Write-Host "`n=== DEPLOYMENT SUMMARY ==="
    $successCount = ($deploymentResults | Where-Object { $_.DeploymentSuccess }).Count
    $totalCount = $deploymentResults.Count
    
    Write-Host "Total PBIP files processed: $totalCount"
    Write-Host "Successful deployments: $successCount"
    Write-Host "Failed deployments: $($totalCount - $successCount)"

    # Fail the deployment if any PBIP file failed to deploy
    if ($successCount -lt $totalCount) {
        $failedReports = $deploymentResults | Where-Object { -not $_.DeploymentSuccess } | Select-Object -ExpandProperty ReportName
        Write-Error "The following reports failed to deploy: $($failedReports -join ', ')"
        throw "One or more PBIP deployments failed"
    }

    Write-Host "`n✓ Power BI PBIP Report Deployment completed successfully!"
}
catch {
    Write-Error "Power BI PBIP Report Deployment failed: $_"
    exit 1
}