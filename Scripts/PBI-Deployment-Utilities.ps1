# PBI-Deployment.ps1 for PBIP file deployment (with refresh, takeover, and validation logic)

try {
    # Import utility scripts with error handling
    . "$PSScriptRoot\Token-Utilities.ps1"
    . "$PSScriptRoot\PBI-Deployment-Utilities.ps1"
} catch {
    Write-Warning "Could not import utility scripts: $_"
    Write-Host "Attempting to continue without utility scripts..."
}

function Get-PBIPFiles {
    param(
        $ArtifactPath,  # Base path where the artifact is stored
        $Folder         # Subfolder to search in
    )

    # Combine base path and subfolder to form the full target path
    if ($Folder) {
        $target = Join-Path $ArtifactPath $Folder
    } else {
        $target = $ArtifactPath
    }

    # Check if the target path exists
    if (-not (Test-Path $target)) {
        Write-Warning "Path not found: $target"
        return @()
    }

    # Recursively find all .pbip files in the target directory
    $files = Get-ChildItem -Path $target -Recurse -File -Filter '*.pbip'
    Write-Host "Found $($files.Count) PBIP files in $target"
    
    foreach ($file in $files) {
        Write-Host "  Found PBIP: $($file.FullName)"
        
        # Check for associated folders
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
        
        # Check for key files
        $reportDefFile = Join-Path $reportFolder "report.json"
        $modelBimFile = Get-ChildItem -Path $semanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        
        Write-Host "    Report definition: $(Test-Path $reportDefFile)"
        Write-Host "    Model BIM file: $($modelBimFile -ne $null)"
        
        return @{
            IsValid = $true
            ReportFolder = $reportFolder
            SemanticModelFolder = $semanticModelFolder
            ReportDefFile = $reportDefFile
            ModelBimFile = $modelBimFile.FullName
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
        [Parameter(Mandatory=$true)]
        [string]$Takeover
    )
    
    try {
        Write-Host "Starting Fabric API deployment for PBIP: $ReportName"
        
        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        # Validate PBIP structure
        $validation = Validate-PBIPStructure -PBIPFilePath $PBIPFilePath
        if (-not $validation.IsValid) {
            throw "Invalid PBIP structure for: $ReportName"
        }
        
        Write-Host "PBIP structure validated successfully"
        
        # Step 1: Deploy Semantic Model first
        Write-Host "Deploying semantic model..."
        $semanticModelSuccess = Deploy-SemanticModel -SemanticModelFolder $validation.SemanticModelFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ModelName $ReportName
        
        if (-not $semanticModelSuccess) {
            Write-Warning "Semantic model deployment failed"
            return $false
        }
        
        # Step 2: Deploy Report
        Write-Host "Deploying report..."
        $reportSuccess = Deploy-Report -ReportFolder $validation.ReportFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ReportName $ReportName
        
        if (-not $reportSuccess) {
            Write-Warning "Report deployment failed"
            return $false
        }
        
        # If takeover is true, do the takeover after deployment
        if ($Takeover -eq "True") {
            Takeover-PBIP -ReportName $ReportName -WorkspaceId $WorkspaceId -AccessToken $AccessToken
        }
        
        # Refresh the report after deployment
        Refresh-PBIP -ReportName $ReportName -WorkspaceId $WorkspaceId -AccessToken $AccessToken

        Write-Host "✓ PBIP deployment completed successfully for: $ReportName"
        return $true
        
    } catch {
        Write-Error "Fabric API deployment failed for $ReportName : $_"
        return $false
    }
}

function Takeover-PBIP {
    param(
        [string]$ReportName,
        [string]$WorkspaceId,
        [string]$AccessToken
    )

    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$ReportName/takeover"
    
    $headers = @{
        "Authorization" = "Bearer $AccessToken"
        "Content-Type"  = "application/json"
    }

    try {
        $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers
        Write-Host "Report takeover completed successfully: $ReportName"
    } catch {
        Write-Error "Failed to takeover report: $_"
    }
}

function Refresh-PBIP {
    param(
        [string]$ReportName,
        [string]$WorkspaceId,
        [string]$AccessToken
    )

    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$ReportName/refreshes"
    
    $headers = @{
        "Authorization" = "Bearer $AccessToken"
        "Content-Type"  = "application/json"
    }

    try {
        $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers
        Write-Host "Report refreshed successfully: $ReportName"
    } catch {
        Write-Error "Failed to refresh report: $_"
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
        
        # Find model.bim file
        $modelBimFile = Get-ChildItem -Path $SemanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        
        if (-not $modelBimFile) {
            throw "model.bim file not found in semantic model folder"
        }
        
        # Read model definition
        $modelDefinition = Get-Content $modelBimFile.FullName -Raw
        Write-Host "Model definition loaded: $($modelDefinition.Length) characters"
        
        # Prepare deployment payload
        $deploymentPayload = @{
            "name" = $ModelName
            "definition" = @{
                "parts" = @(
                    @{
                        "path" = "model.bim"
                        "payload" = $modelDefinition
                        "payloadType" = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10
        
        # Deploy semantic model using Fabric API
        $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
        
        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        try {
            $response = Invoke-RestMethod -Uri $deployUrl -Method Post -Body $deploymentPayload -Headers $headers
            Write-Host "✓ Semantic model deployed successfully"
            return $true
        } catch {
            if ($_.Exception.Response.StatusCode -eq 409) {
                Write-Host "Semantic model already exists, attempting update..."
                
                # Try to update existing model
                $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$ModelName"
                try {
                    $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Patch -Body $deploymentPayload -Headers $headers
                    Write-Host "✓ Semantic model updated successfully"
                    return $true
                } catch {
                    Write-Warning "Failed to update semantic model: $_"
                    return $false
                }
            } else {
                throw $_
            }
        }
        
    } catch {
        Write-Error "Failed to deploy semantic model: $_"
        return $false
    }
}

function Initialize-PowerShellEnvironment {
    Write-Host "Initializing PowerShell environment for PBIP deployment..."
    
    try {
        # Ensure TLS 1.2 is enabled
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        
        # Check if required modules are available
        $requiredModules = @('MicrosoftPowerBIMgmt', 'Az.Accounts')
        
        foreach ($module in $requiredModules) {
            $moduleAvailable = Get-Module -Name $module -ListAvailable -ErrorAction SilentlyContinue
            if (-not $moduleAvailable) {
                Write-Warning "Module $module is not available. Attempting to install..."
                
                try {
                    # Try to install the module
                    Install-Module -Name $module -Force -AllowClobber -Scope CurrentUser -SkipPublisherCheck -Repository PSGallery -Confirm:$false
                    Write-Host "✓ Successfully installed $module"
                } catch {
                    Write-Error "Failed to install $module : $_"
                    throw "Required module $module could not be installed"
                }
            } else {
                Write-Host "✓ Module $module is available (Version: $($moduleAvailable.Version))"
            }
            
            # Import the module
            try {
                Import-Module -Name $module -Force -ErrorAction Stop
                Write-Host "✓ Successfully imported $module"
            } catch {
                Write-Error "Failed to import $module : $_"
                throw "Required module $module could not be imported"
            }
        }
        
        Write-Host "PowerShell environment initialized successfully"
        return $true
    }
    catch {
        Write-Error "Failed to initialize PowerShell environment: $_"
        return $false
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
    
    try {
        Write-Host "Acquiring access token automatically..."
        
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
        
    } catch {
        Write-Error "Failed to acquire access token: $_"
        
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
        } catch {
            Write-Error "Failed to acquire fallback access token: $_"
            throw "Could not acquire any access token"
        }
    }
}

function Invoke-ReportDeployment {
    param(
        [Parameter(Mandatory=$true)]
        [string]$Workspace,
        
        [Parameter(Mandatory=$true)]
        [string]$ConfigFile
    )

    try {
        Write-Host "Starting Power BI PBIP Report Deployment..."
        Write-Host "Environment: $Workspace"
        Write-Host "Config File: $ConfigFile"

        # Initialize PowerShell environment
        $envInitialized = Initialize-PowerShellEnvironment
        if (-not $envInitialized) {
            throw "Failed to initialize PowerShell environment"
        }

        # Read configuration file
        if (-not (Test-Path $ConfigFile)) {
            throw "Configuration file not found: $ConfigFile"
        }

        $config = Get-Content $ConfigFile | ConvertFrom-Json
        Write-Host "Configuration loaded successfully"

        # Set environment variables from config
        $deployment_env = $Workspace
        $deployment_env_lower = $deployment_env.ToLower()

        # Get SPN credentials from config
        $tenantId = $config.TenantID
        $clientId = $config.ClientID
        $clientSecret = $config.ClientSecret

        Write-Host "Using Tenant ID: $tenantId"
        Write-Host "Using Client ID: $clientId"

        # Map workspace based on environment (only Dev and Prod supported)
        $targetWorkspaceId = $null
        $targetWorkspaceName = $null
        
        switch ($deployment_env.ToUpper()) {
            "DEV" {
                $targetWorkspaceId = $config.DevWorkspaceID
                $targetWorkspaceName = "Dev Workspace"
            }
            "PROD" {
                $targetWorkspaceId = $config.UATWorkspaceID
                $targetWorkspaceName = "Prod Workspace"
            }
            default {
                throw "Unsupported environment: $deployment_env. Only DEV and PROD are supported."
            }
        }

        if (-not $targetWorkspaceId) {
            throw "Workspace ID not found for environment: $deployment_env"
        }

        Write-Host "Target Workspace ID: $targetWorkspaceId"
        Write-Host "Target Workspace Name: $targetWorkspaceName"

        # Get Access Token (Automatic - No manual token needed)
        $accessToken = Get-AccessTokenFromConfig -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret

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

        # List found files for debugging
        foreach ($file in $allPbipFiles) {
            Write-Host "  Found PBIP file: $($file.FullName)"
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
            
            # Deploy using Fabric API
            $deploymentSuccess = Deploy-PBIPUsingFabricAPI -PBIPFilePath $pbipFile.FullName -ReportName $reportName -WorkspaceId $targetWorkspaceId -AccessToken $accessToken -Takeover "True"
            
            $result = [PSCustomObject]@{
                ReportName = $reportName
                FilePath = $pbipFile.FullName
                DeploymentSuccess = $deploymentSuccess
            }

            $deploymentResults += $result

            if ($deploymentSuccess) {
                Write-Host "✓ Successfully deployed: $reportName"
            } else {
                Write-Warning "❌ Failed to deploy: $reportName"
            }
        }

        Write-Host "`n=== DEPLOYMENT SUMMARY ==="
        $successCount = ($deploymentResults | Where-Object { $_.DeploymentSuccess }).Count
        $totalCount = $deploymentResults.Count
        
        Write-Host "Total PBIP files processed: $totalCount"
        Write-Host "Successful deployments: $successCount"
        Write-Host "Failed deployments: $($totalCount - $successCount)"
        
    }
    catch {
        Write-Error "Power BI PBIP Report Deployment failed: $_"
        exit 1
    }
}

# Main execution
param(
    [string]$Workspace,
    [string]$ConfigFile
)

Invoke-ReportDeployment -Workspace $Workspace -ConfigFile $ConfigFile
