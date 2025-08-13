# Import utility scripts
. "$PSScriptRoot\Token-Utilities.ps1"
. "$PSScriptRoot\PBI-Deployment-Utilities.ps1"

function Get-PBIXFiles {
    param(
        $ArtifactPath,  # Base path where the artifact is stored
        $Folder         # Subfolder (e.g., "Reporting" or "Operations") to search in
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

        # Define configuration paths
        $config_base_path = "Configuration/$deployment_env"
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
        $accessToken = Get-SPNToken $tenantId $clientId $clientSecret
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

        # Get PBIX files
        $reportingPbixFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder "Demo Reporting/Reporting"
        $operationsPbixFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder "Demo Reporting/Operations"

        Write-Host "Found $($reportingPbixFiles.Count) reporting PBIX files"
        Write-Host "Found $($operationsPbixFiles.Count) operations PBIX files"

        # Connect to Power BI
        Write-Host "Connecting to Power BI Service..."
        Connect-PowerBIServiceAccount -ServicePrincipal -TenantId $tenantId -Credential $credential

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
                
                if ($workspace.Name -like "*operations*") {
                    $operationsWorkspaceId = $workspace.Id
                    $operationsWorkspaceName = $workspace.Name
                }
                
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

            # Determine workspace type and files to process
            if ($workspaceName -like "*operations*") {
                $PbixFiles = $operationsPbixFiles
                $workspaceType = "operations"
                $sqlDatabases = @("Configuration", "ArchiveLogs")
            } else {
                $PbixFiles = $reportingPbixFiles
                $workspaceType = "reporting"
                $sqlDatabases = @("Reporting_Homepage")
            }

            Write-Host "Workspace type: $workspaceType"
            Write-Host "PBIX files to process: $($PbixFiles.Count)"

            try {
                # Create SQL cloud connections for lakehouses (simplified approach)
                Write-Host "Creating cloud connections..."
                
                foreach ($lakehouseName in $sqlDatabases) {
                    try {
                        # Get lakehouses in workspace
                        $lakehouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/lakehouses" -Headers $headers -ErrorAction SilentlyContinue).value
                        
                        if ($lakehouses) {
                            $lh = $lakehouses | Where-Object { $_.displayName -eq $lakehouseName } | Select-Object -First 1
                            
                            if ($lh) {
                                # Get lakehouse details
                                $details = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/lakehouses/$($lh.id)" -Headers $headers
                                $sqlProps = $details.properties.sqlEndpointProperties
                                $sqlConnectionString = "Server=$($sqlProps.connectionString);Database=$($details.displayName);"
                                
                                # Create SQL cloud connection
                                $connectionDisplayName = "t360-udp-$deployment_env_lower-$lakehouseName-$workspaceId"
                                
                                # Store connection details
                                $connectionMap[$lakehouseName] = $sqlProps.connectionString
                                Write-Host "Created connection mapping for: $lakehouseName"
                            }
                        }
                    }
                    catch {
                        Write-Warning "Could not process lakehouse $lakehouseName : $_"
                    }
                }

                # Process PBIX files
                foreach ($file in $PbixFiles) {
                    $PowerBIReportFilePath = $file.FullName
                    $reportName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)

                    Write-Host "`n  Processing report: $reportName"
                    Write-Host "  File path: $PowerBIReportFilePath"

                    try {
                        # Deploy Report
                        Write-Host "  Deploying report..."
                        $response = Create-PowerBI-Report -AccessToken $accessToken `
                                -PowerBIReportFilePath $PowerBIReportFilePath `
                                -PowerBIReportName $reportName `
                                -WorkspaceId $workspaceId

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
                        Invoke-PowerBIRestMethod -Method Post -Url $takeoverUrl

                        # Refresh Dataset
                        Write-Host "  Refreshing dataset..."
                        Refresh-PowerBI-Datasets -WorkspaceId $workspaceId -datasetId $datasetId -WaitTillPowerBIDatasetRefresh $WaitTillPowerBIDatasetRefresh

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