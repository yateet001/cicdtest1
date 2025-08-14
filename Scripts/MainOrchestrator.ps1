# Import utility scripts with error handling
try {
    . "$PSScriptRoot\Token-Utilities.ps1"
    . "$PSScriptRoot\PBI-Deployment-Utilities.ps1"
} catch {
    Write-Warning "Could not import utility scripts: $_"
    Write-Host "Attempting to continue without utility scripts..."
}

function Get-PBIXFiles {
    param(
        $ArtifactPath,  # Base path where the artifact is stored
        $Folder         # Subfolder to search in
    )

    # Combine base path and subfolder to form the full target path
    $target = Join-Path $ArtifactPath $Folder

    # Check if the target path exists; throw an error if it doesn't
    if (-not (Test-Path $target)) {
        Write-Warning "Path not found: $target"
        return @()  # Return empty array instead of throwing error
    }

    # Recursively find all .pbix files in the target directory
    $files = Get-ChildItem -Path $target -Recurse -File -Filter '*.pbix'

    return $files
}

function Initialize-PowerShellEnvironment {
    Write-Host "Initializing PowerShell environment..."
    
    try {
        # Ensure TLS 1.2 is enabled
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        
        # Check if required modules are available
        $requiredModules = @('MicrosoftPowerBIMgmt')
        
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

function Invoke-ReportDeployment {
    param(
        [Parameter(Mandatory=$true)]
        [string]$Workspace,
        
        [Parameter(Mandatory=$true)]
        [string]$ConfigFile
    )

    try {
        Write-Host "Starting Power BI Report Deployment..."
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

        # Define configuration paths - Updated for root-level structure
        $config_base_path = "Configuration/$deployment_env"
        $deployment_profile_path = "$config_base_path/DEPLOYMENT_PROFILE.csv"

        # Check if deployment profile exists, if not create a default one
        if (-not (Test-Path $deployment_profile_path)) {
            Write-Warning "Deployment profile not found: $deployment_profile_path"
            Write-Host "Creating default deployment profile..."
            
            # Create Configuration directory structure
            $configDir = Split-Path $deployment_profile_path -Parent
            if (-not (Test-Path $configDir)) {
                New-Item -Path $configDir -ItemType Directory -Force | Out-Null
                Write-Host "Created directory: $configDir"
            }
            
            # Create default deployment profile
            $defaultProfile = @"
workspace_name,report_name,report_path,warehouse_name,environment_type,transformation_layer
${deployment_env}Workspace1,Demo Report,Demo Report/Demo Report.pbix,WH_$deployment_env,Reporting,Reporting
"@
            $defaultProfile | Out-File -FilePath $deployment_profile_path -Encoding UTF8
            Write-Host "Created default deployment profile: $deployment_profile_path"
        }

        Write-Host "Reading deployment profile: $deployment_profile_path"

        # Read deployment profile CSV
        $deployment_profile = Import-Csv -Path $deployment_profile_path
        Write-Host "Found $($deployment_profile.Count) entries in deployment profile"

        # Process the deployment profile to add workspace mapping
        foreach ($entry in $deployment_profile) {
            # Map workspace names based on environment
            if ($deployment_env -eq "Dev") {
                $entry | Add-Member -NotePropertyName "mapped_workspace_name" -NotePropertyValue "DevWorkspace1" -Force
                $entry | Add-Member -NotePropertyName "mapped_workspace_id" -NotePropertyValue $config.DevWorkspaceID -Force
            }
            elseif ($deployment_env -eq "UAT") {
                $entry | Add-Member -NotePropertyName "mapped_workspace_name" -NotePropertyValue "UATWorkspace1" -Force
                $entry | Add-Member -NotePropertyName "mapped_workspace_id" -NotePropertyValue $config.UATWorkspaceID -Force
            }
            else {
                throw "Unsupported environment: $deployment_env"
            }
        }

        # Get Access Token
        Write-Host "Getting access token..."
        
        # Alternative token acquisition method (since utility scripts may not be available)
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $clientId
            client_secret = $clientSecret
            resource      = "https://analysis.windows.net/powerbi/api"
        }
        
        try {
            $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$tenantId/oauth2/token" -Method Post -Body $body
            $accessToken = $tokenResponse.access_token
            Write-Host "✓ Successfully acquired access token"
        } catch {
            throw "Failed to acquire access token: $_"
        }
        
        $headers = @{ Authorization = "Bearer $accessToken" }

        # Secure the client secret
        $securePassword = ConvertTo-SecureString $clientSecret -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential ($clientId, $securePassword)

        # Set artifact path (assuming build sources directory)
        $artifactPath = $env:BUILD_SOURCESDIRECTORY
        if (-not $artifactPath) {
            $artifactPath = (Get-Location).Path
        }
        Write-Host "Using artifact path: $artifactPath"

        # Get PBIX files - Updated paths for root-level structure
        $reportFolders = @("Demo Report", "Reporting", "Reports")
        $allPbixFiles = @()
        
        foreach ($folder in $reportFolders) {
            $folderPath = Join-Path $artifactPath $folder
            if (Test-Path $folderPath) {
                $pbixFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder $folder
                $allPbixFiles += $pbixFiles
                Write-Host "Found $($pbixFiles.Count) PBIX files in $folder folder"
            }
        }

        # If no PBIX files found in specific folders, search entire repository
        if ($allPbixFiles.Count -eq 0) {
            Write-Host "No PBIX files found in expected folders, searching entire repository..."
            $allPbixFiles = Get-ChildItem -Path $artifactPath -Recurse -Filter "*.pbix" -ErrorAction SilentlyContinue
            Write-Host "Found $($allPbixFiles.Count) PBIX files total"
        }

        # List found files for debugging
        foreach ($file in $allPbixFiles) {
            Write-Host "  Found PBIX file: $($file.FullName)"
        }

        if ($allPbixFiles.Count -eq 0) {
            Write-Warning "No PBIX files found. Looking for PBIP files..."
            $pbipFiles = Get-ChildItem -Path $artifactPath -Recurse -Filter "*.pbip" -ErrorAction SilentlyContinue
            Write-Host "Found $($pbipFiles.Count) PBIP files"
            foreach ($file in $pbipFiles) {
                Write-Host "  Found PBIP file: $($file.FullName)"
            }
            
            if ($pbipFiles.Count -eq 0) {
                throw "No Power BI report files (.pbix or .pbip) found in the repository"
            }
        }

        # Connect to Power BI
        Write-Host "Connecting to Power BI Service..."
        
        try {
            Connect-PowerBIServiceAccount -ServicePrincipal -TenantId $tenantId -Credential $credential
            Write-Host "✓ Successfully connected to Power BI Service"
        } catch {
            Write-Error "Failed to connect to Power BI Service: $_"
            throw
        }

        # Get all Power BI workspaces
        $allWorkspaces = Get-PowerBIWorkspace -All
        Write-Host "Retrieved $($allWorkspaces.Count) workspaces"

        # Initialize variables
        $workspaceDatasetInfo = @{}
        $WaitTillPowerBIDatasetRefresh = "True"
        $workspaceDetails = @()

        # Get unique workspaces from deployment profile
        $uniqueWorkspaces = $deployment_profile | Select-Object mapped_workspace_name, mapped_workspace_id -Unique

        foreach ($wsInfo in $uniqueWorkspaces) {
            $workspace = $allWorkspaces | Where-Object { $_.Id -eq $wsInfo.mapped_workspace_id }
            
            if ($workspace) {
                Write-Host "Found workspace: $($workspace.Name) (ID: $($workspace.Id))"
                
                $workspaceDetails += @{
                    Id   = $workspace.Id
                    Name = $workspace.Name
                }
            } else {
                Write-Warning "Workspace not found: $($wsInfo.mapped_workspace_name) (ID: $($wsInfo.mapped_workspace_id))"
            }
        }

        # Process each workspace
        foreach ($entry in $workspaceDetails) {
            $workspaceId = $entry.Id
            $workspaceName = $entry.Name

            Write-Host "`nProcessing workspace: $workspaceName (ID: $workspaceId)"

            # Use all found PBIX files
            $PbixFiles = $allPbixFiles
            $workspaceType = "reports"

            Write-Host "Workspace type: $workspaceType"
            Write-Host "PBIX files to process: $($PbixFiles.Count)"

            try {
                # Process PBIX files
                foreach ($file in $PbixFiles) {
                    $PowerBIReportFilePath = $file.FullName
                    $reportName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)

                    Write-Host "`n  Processing report: $reportName"
                    Write-Host "  File path: $PowerBIReportFilePath"

                    try {
                        # Deploy Report using PowerShell module method
                        Write-Host "  Deploying report..."
                        
                        try {
                            New-PowerBIReport -Path $PowerBIReportFilePath -Name $reportName -WorkspaceId $workspaceId
                            Write-Host "  ✓ Report deployed successfully"
                        } catch {
                            if ($_.Exception.Message -like "*already exists*") {
                                Write-Host "  ℹ Report already exists, attempting to update..."
                                # Try to get the existing report and update it
                                $existingReports = Get-PowerBIReport -WorkspaceId $workspaceId | Where-Object { $_.Name -eq $reportName }
                                if ($existingReports) {
                                    Write-Host "  ℹ Found existing report, skipping deployment"
                                } else {
                                    throw $_
                                }
                            } else {
                                throw $_
                            }
                        }

                        # Get Dataset
                        Write-Host "  Getting dataset..."
                        Start-Sleep -Seconds 5  # Wait for deployment to complete
                        $dataset = Get-PowerBIDataset -WorkspaceId $workspaceId | Where-Object { $_.Name -eq $reportName }
                        if (-not $dataset) {
                            Write-Warning "Dataset $reportName not found in workspace $workspaceId"
                            continue
                        }
                        $datasetId = $dataset.Id
                        Write-Host "  Dataset ID: $datasetId"

                        # Take Over Dataset
                        Write-Host "  Taking over dataset..."
                        try {
                            $takeoverUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.TakeOver"
                            Invoke-PowerBIRestMethod -Method Post -Url $takeoverUrl
                            Write-Host "  ✓ Dataset takeover successful"
                        } catch {
                            Write-Warning "Could not take over dataset (this may be normal): $_"
                        }

                        # Refresh Dataset
                        Write-Host "  Refreshing dataset..."
                        try {
                            $refreshUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/refreshes"
                            Invoke-PowerBIRestMethod -Method Post -Url $refreshUrl
                            Write-Host "  ✓ Dataset refresh initiated successfully"
                        } catch {
                            Write-Warning "Could not refresh dataset: $_"
                        }

                        Write-Host "  ✓ Report $reportName processed successfully"
                    }
                    catch {
                        Write-Error "  ✗ Failed to process report $reportName : $_"
                        # Continue with other reports instead of failing completely
                        continue
                    }
                }
            }
            catch {
                Write-Error "Failed to process workspace $workspaceName : $_"
                throw
            }
        }

        Write-Host "`n✓ Power BI Report Deployment completed successfully!"
    }
    catch {
        Write-Error "Power BI Report Deployment failed: $_"
        throw
    }
}

# Main execution
if ($MyInvocation.InvocationName -ne '.') {
    # Get parameters from command line arguments
    param(
        [Parameter(Mandatory=$true)]
        [string]$Workspace,
        
        [Parameter(Mandatory=$true)]
        [string]$ConfigFile
    )
    
    try {
        Invoke-ReportDeployment -Workspace $Workspace -ConfigFile $ConfigFile
    }
    catch {
        Write-Error "An error occurred while orchestrating Power BI Deployment: $_"
        exit 1
    }
}