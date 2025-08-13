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
        $Folder         # Subfolder (e.g., "Demo Report") to search in
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

        # Define configuration paths - Updated to match your folder structure
        $config_base_path = "Visa/Configuration/$deployment_env"
        $deployment_profile_path = "$config_base_path/DEPLOYMENT_PROFILE.csv"

        if (-not (Test-Path $deployment_profile_path)) {
            throw "Deployment profile not found: $deployment_profile_path"
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
        
        # Check if Get-SPNToken function is available, otherwise use alternative method
        if (Get-Command Get-SPNToken -ErrorAction SilentlyContinue) {
            $accessToken = Get-SPNToken $tenantId $clientId $clientSecret
        } else {
            Write-Warning "Get-SPNToken function not available, using alternative token method..."
            # Alternative token acquisition method
            $body = @{
                grant_type    = "client_credentials"
                client_id     = $clientId
                client_secret = $clientSecret
                resource      = "https://analysis.windows.net/powerbi/api"
            }
            
            try {
                $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$tenantId/oauth2/token" -Method Post -Body $body
                $accessToken = $tokenResponse.access_token
                Write-Host "✓ Successfully acquired access token using alternative method"
            } catch {
                throw "Failed to acquire access token: $_"
            }
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

        # Get PBIX files - Updated paths to match your folder structure
        $demoReportPath = "Visa/Demo Report"
        $demoReportFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder $demoReportPath

        Write-Host "Found $($demoReportFiles.Count) PBIX files in Demo Report folder"

        # List found files for debugging
        foreach ($file in $demoReportFiles) {
            Write-Host "  Found file: $($file.FullName)"
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
        $connectionIds = @()
        $connectionMap = @{}

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

            # Use all PBIX files from Demo Report folder
            $PbixFiles = $demoReportFiles
            $workspaceType = "demo_reports"

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
                        # Deploy Report
                        Write-Host "  Deploying report..."
                        
                        # Check if Create-PowerBI-Report function is available
                        if (Get-Command Create-PowerBI-Report -ErrorAction SilentlyContinue) {
                            $response = Create-PowerBI-Report -AccessToken $accessToken `
                                    -PowerBIReportFilePath $PowerBIReportFilePath `
                                    -PowerBIReportName $reportName `
                                    -WorkspaceId $workspaceId
                        } else {
                            Write-Warning "Create-PowerBI-Report function not available, using alternative deployment method..."
                            # Use PowerShell module method
                            New-PowerBIReport -Path $PowerBIReportFilePath -Name $reportName -WorkspaceId $workspaceId
                        }

                        # Get Dataset
                        Write-Host "  Getting dataset..."
                        $dataset = Get-PowerBIDataset -WorkspaceId $workspaceId | Where-Object { $_.Name -eq $reportName }
                        if (-not $dataset) {
                            throw "Dataset $reportName not found in workspace $workspaceId"
                        }
                        $datasetId = $dataset.Id
                        Write-Host "  Dataset ID: $datasetId"

                        # Take Over Dataset
                        Write-Host "  Taking over dataset..."
                        $takeoverUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.TakeOver"
                        try {
                            Invoke-PowerBIRestMethod -Method Post -Url $takeoverUrl
                        } catch {
                            Write-Warning "Could not take over dataset (this may be normal): $_"
                        }

                        # Refresh Dataset
                        Write-Host "  Refreshing dataset..."
                        
                        # Check if Refresh-PowerBI-Datasets function is available
                        if (Get-Command Refresh-PowerBI-Datasets -ErrorAction SilentlyContinue) {
                            Refresh-PowerBI-Datasets -WorkspaceId $workspaceId -datasetId $datasetId -WaitTillPowerBIDatasetRefresh $WaitTillPowerBIDatasetRefresh
                        } else {
                            Write-Warning "Refresh-PowerBI-Datasets function not available, using alternative refresh method..."
                            try {
                                $refreshUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/refreshes"
                                Invoke-PowerBIRestMethod -Method Post -Url $refreshUrl
                                Write-Host "  Dataset refresh initiated successfully"
                            } catch {
                                Write-Warning "Could not refresh dataset: $_"
                            }
                        }

                        Write-Host "  ✓ Report $reportName processed successfully"
                    }
                    catch {
                        Write-Error "  ✗ Failed to process report $reportName : $_"
                        throw
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