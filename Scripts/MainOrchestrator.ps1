# Import utility scripts
. "$PSScriptRoot\Token-Utilities.ps1"
. "$PSScriptRoot\PBI-Deployment-Utilities.ps1"

function Get-PBIXFiles {
    param(
        $ArtifactPath,  # Base path where the artifact is stored
        $Folder         # Subfolder (e.g., "Reporting" or "Operations") to search in
    )

    # Read environment variables into PowerShell variables
    $artifactPath   = $env:artifact_path
    $buildNumber    = $env:build_number

    # Combine base path and subfolder to form the full target path
    $target = Join-Path $ArtifactPath $Folder

    # Check if the target path exists; throw an error if it doesn't
    if (-not (Test-Path $target)) {
        throw "Missing $target"
    }

    # Recursively find all .pbix files in the target directory
    # Returns FileInfo objects with properties like .Name and .FullName
    $files = Get-ChildItem -Path $target -Recurse -File -Filter '*.pbix'

    # Return the list of found .pbix files
    return $files
}


function Invoke-ReportDeployment {
    <#
    .SYNOPSIS
    Deploys Power BI report, updates datasources, creates cloud connections, binds datasets, and triggers refresh.

    .DESCRIPTION
    Handles the end-to-end deployment of Power BI reports across filtered workspaces using configuration and secrets provided.

    .PARAMETER deployment_env
    Deployment environment variable (picked from environment).

    .EXAMPLE
    Invoke-ReportDeployment
    #>


    # Get deployment environment variable
    $deployment_env = $env:deployment_env

    # Get the environment variable value and parse JSON safely
    $udp_t360_spn = $env:udp_t360_spn -replace "'", '"'
    $key_vault_spn_secrets = $udp_t360_spn | ConvertFrom-Json

    # Extract values from SPN secrets
    $tenantId = $key_vault_spn_secrets.tenant_id
    $clientId = $key_vault_spn_secrets.client_id
    $clientSecret = $key_vault_spn_secrets.client_secret
    $deployment_env_lower = $deployment_env.ToLower()
    $build_number = $env:build_number  
    $environmentType = $env:environment_type

    # Define configuration paths
    $config_base_path = "Configuration/$deployment_env"
    $deployment_profile_path = "$config_base_path/DEPLOYMENT_PROFILE.csv"
    $deployment_code = (Get-Date -AsUTC).ToString("yyyyMMddHHmmss")
    
    # Read deployment profile CSV and filter by environment_type
    $all_deployment_profile = Import-Csv -Path $deployment_profile_path
    $filtered_deployment_profile = $all_deployment_profile | Where-Object { $_.environment_type -eq $environmentType }

    # Add ws_name field only for reporting workspaces
    $all_deployment_profile | ForEach-Object { 
        $nid = 0
        if (-not [int]::TryParse($_.network_id, [ref]$nid)) {
            return  
        }

        # strip any “-C” followed by digits at the end
        $clean_prefix = $_.workspace_prefix -replace "-C\d*$",""
        $transformationLayer = $_.transformation_layer

        if ($transformationLayer -like "*Reporting*" -and $nid -ge 0) {
            # build new 5-digit C-suffix
            $nid_suffix = "C{0:D5}" -f $nid
            $_ | Add-Member -NotePropertyName ws_name `
                        -NotePropertyValue ("$clean_prefix-$nid_suffix") `
                        -Force
        }
        else {
            $_ | Add-Member -NotePropertyName ws_name `
                        -NotePropertyValue $clean_prefix `
                        -Force
        }

    }

    # Filter processed workspaces (reporting and operations workspaces)
    $filtered_profiles = $all_deployment_profile | Where-Object {
        $_.transformation_layer -like "*Reporting*" -or $_.transformation_layer -like "*Operations*" 
    } | Select-Object -ExpandProperty ws_name

    # Get Access Token
    $accessToken = Get-SPNToken $tenantId $clientId $clientSecret
    $headers = @{ Authorization = "Bearer $accessToken" }

    # Secure the client secret
    $securePassword = ConvertTo-SecureString $clientSecret -AsPlainText -Force
    $credential = New-Object System.Management.Automation.PSCredential ($clientId, $securePassword)

    # Get all PBIX files   
    $reportingPbixFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder "Demo Reporting/Reporting"
    $operationsPbixFiles = Get-PBIXFiles -ArtifactPath $artifactPath -Folder "Demo Reporting/Operations"    

    # Connect to Power BI
    Connect-PowerBIServiceAccount -ServicePrincipal -TenantId $tenantId -Credential $credential

    # Get all Power BI workspaces
    $allWorkspaces = Get-PowerBIWorkspace -All  

    $workspaceDatasetInfo = @{}
    $WaitTillPowerBIDatasetRefresh = "True"
    $AppId = ""

    # Initialize array to store workspace details
    $workspaceDetails = @()
    # Initialize array to store connection IDs
    $connectionIds = @()
    # Initialize a hashtable to map connection strings to connection IDs
    $connectionMap = @{}

    # Identify workspace IDs and operations workspace
    foreach ($ws_name in $filtered_profiles) {

        $workspace = $allWorkspaces | Where-Object { $_.Name -eq $ws_name }

        if ($workspace) {

            if ($workspace.Name -like "*-operations") {
                $operationsWorkspaceId = $workspace.Id
                $operationsWorkspaceName = $workspace.Name
            } 
            # Store both ID and Name together as a hashtable/object
            $workspaceDetails += @{
                Id   = $workspace.Id
                Name = $workspace.Name
            }
        } else {
            throw "No matching workspace found for profile: $ws_name"
        }
    }
    
    # Get KQL URI using API for the dashboard
    $eventhouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$operationsWorkspaceId/eventhouses" -Headers $headers).value
    if (-not $eventhouses) { 
        throw "##[error] No Eventhouse found in workspace ${operationsWorkspaceId})"
    }
    $newKustoUrl = $eventhouses[0].properties.queryServiceUri
    $newKustoDatabase = "executionlogs"

    try {
        $errorMessage = ""
        # Process each stored workspace entry
        foreach ($entry in $workspaceDetails) {
            
            $workspaceId = $entry.Id
            $workspaceName = $entry.Name

            # Create a new variable that is assigned based on the workspace name
            if ($workspaceName -like "*operations*") {
                $PbixFiles = $operationsPbixFiles  # If workspace name contains 'operations', use operations list
                $workspaceType = "operations"
                $sqlDatabases = @("Configuration", "ArchiveLogs")
            } else {
                $PbixFiles = $reportingPbixFiles  # Otherwise, use reporting list
                $workspaceType = "reporting"
                $sqlDatabases = @("Reporting_Homepage")
            }

            # Get the path to the folder where this script is located
            $psScriptFolder = $PSScriptRoot

            # Get the parent folder (i.e. the COMMON folder where logging_cicd.py is located)
            $modulePath = (Resolve-Path "$psScriptFolder\..").Path
            
            try {       
                try {
                    # Retrieve Lakehouses
                    $lakehouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/lakehouses" -Headers $headers).value
                    if (-not $lakehouses) { 
                        continue
                    }

                    # Create SQL cloud connections for each lakehouse
                    foreach ($lakehouseName in $sqlDatabases){                        
                        # Retrieve and find the lakehouse
                        $lakehouses = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/lakehouses" -Headers $headers).value
                        if (-not $lakehouses) { 
                            continue
                        }

                        # Get details of current Lakehouse
                        $lh = $lakehouses | Where-Object { $_.displayName -eq $lakehouseName }

                        # Get SQL endpoint and build connection string
                        $details = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/lakehouses/$($lh.id)" -Headers $headers
                        $sqlProps = $details.properties.sqlEndpointProperties
                        $sqlConnectionString = "Server=$($sqlProps.connectionString);Database=$($details.displayName);"
                        
                        # Create SQL cloud connection
                        $connectionDisplayName = "t360-udp-$deployment_env_lower-$lakehouseName-$workspaceId"
                        $connectionId = New-FabricSQLCloudConnection -AccessToken $accessToken -ConnectionDisplayName $connectionDisplayName -SqlConnectionString $sqlConnectionString

                        # Append connection IDs
                        $connectionIds += $connectionId
                        # Map the SQL connection string to the returned connection ID
                        $connectionMap[$lakehouseName] = $sqlProps.connectionString

                    }                
                }
                catch {
                    throw "##[error] Failed to retrieve lakehouse details in workspace ${workspaceId}: $($_.Exception.Message)"
                }
                if ($workspaceType -eq "operations"){               
                    try {
                        # Create KQL Cloud Connection
                        $kqlConnectionId = New-FabricKQLCloudConnection `
                            -AccessToken $accessToken `
                            -ConnectionDisplayName "$deployment_env_lower-kusto-$workspaceId" `
                            -KustoUrl $newKustoUrl `
                            -KustoDatabase $newKustoDatabase
                    }
                    catch {
                        throw "##[error] Error creating cloud connection in workspace ${workspaceId}: $($_.Exception.Message)"
                    }
                }

                # Iterate over all reports in the current Workspace
                foreach ($file in $PbixFiles) {
                    $PowerBIReportFilePath = $file.FullName
                    $reportName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)

                    try {
                        # Deploy Report
                        $response = Create-PowerBI-Report -AccessToken $accessToken `
                                -PowerBIReportFilePath $PowerBIReportFilePath `
                                -PowerBIReportName $reportName `
                                -WorkspaceId $workspaceId
                    }
                    catch {
                        throw "##[error] Failed to deploy report in workspace ${workspaceId}: $($_.Exception.Message)"
                    }
                    try {
                        # Get Dataset
                        $dataset = Get-PowerBIDataset -WorkspaceId $workspaceId | Where-Object { $_.Name -eq $reportName }
                        if (-not $dataset) {
                            throw "Dataset $reportName not found in workspace $workspaceId"
                        }
                        $datasetId = $dataset.Id
                    }
                    catch {
                        throw "##[error] Failed to retrieve dataset for $reportName in workspace ${workspaceId}: $($_.Exception.Message)"
                    }
                    
                    try {
                        # Take Over Dataset
                        $takeoverUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.TakeOver"
                        Invoke-PowerBIRestMethod -Method Post -Url $takeoverUrl
                    }
                    catch {
                        throw "##[error] Failed to take over dataset ownership in workspace ${workspaceId}: $($_.Exception.Message)"
                    }                   

                    try {
                        # Get existing bound cloud connections for the dataset
                        $existingConnections = Get-PowerBIDatasource -WorkspaceId $workspaceId -DatasetId $datasetId
                       
                        # Build update URL
                        $updateDatasourceUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.UpdateDatasources"

                        # Extract GatewayId and DatasourceId from JSON where it exists
                        $extDatasource = $existingConnections | Where-Object { $_.DatasourceType -eq "Sql" }

                        $oldConnectionStrings = @(
                            $extDatasource | ForEach-Object { $_.connectionDetails.server }
                        )                            

                        # Select the top value under the assupmtion that connection strings of all Lakehouses are same throughout the same Workspace
                        $oldConnectionString = $oldConnectionStrings[0]

                        # Initialize an empty array to collect all updateDetails
                        $allUpdateDetails = @()                        

                        # Create payload containing old connections and their corresponding new connections to send with update data source REST API call
                        foreach ($lakehouseName in $sqlDatabases) { 
                            # Get the newly created connection string from the map
                            if ($connectionMap.ContainsKey($lakehouseName)) {
                                $connString = $connectionMap[$lakehouseName]
                            }
                            else {
                                throw "No connection-string found for Lakehouse '$lakehouseName'"
                            }

                            # Build the payload object and add it to the array
                            $detail = @{
                                datasourceSelector = @{
                                    datasourceType = "Sql"
                                    connectionDetails = @{
                                        server = $oldConnectionString
                                        database = $lakehouseName
                                    }
                                }
                                connectionDetails = @{
                                    server = $connString
                                    database = $lakehouseName
                                }
                            }
                            $allUpdateDetails += $detail
                        }

                        # Build the final body once, after the loop
                        $bodyHash = @{
                            updateDetails = $allUpdateDetails
                        }

                        $bodyJson = $bodyHash | ConvertTo-Json -Depth 10
             
                        # Send a single UpdateDatasources call
                        $updateDatasourceUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.UpdateDatasources"
                        try {
                            $response = Invoke-PowerBIRestMethod -Method Post -Url $updateDatasourceUrl -Body $bodyJson
                        }
                        catch {                           
                            throw "##[error] Failed to update datasources in dataset ${datasetId}: $($_.Exception.Message)"
                        }
                    }
                    catch {
                        throw "##[error] Failed to update datasource in workspace ${workspaceId}: $($_.Exception.Message)"
                    }
                    
                    if ($workspaceType -eq "operations"){
                        # Update parameters
                        try{
                            # Build the URL
                            $updateParameterUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.UpdateParameters"

                            # Build the body as an array
                            $body = @{
                                updateDetails = @(
                                    @{
                                        name     = "KustoClusterUrl"
                                        newValue = $newKustoUrl
                                    }
                                    @{
                                        name     = "KustoDatabase"
                                        newValue = $newKustoDatabase
                                    }
                                )
                            }

                            # Serialize to JSON
                            $bodyJson = $body | ConvertTo-Json -Depth 10

                            # Make API call
                            $response = Invoke-PowerBIRestMethod `
                                -Method Post `
                                -Url $updateParameterUrl `
                                -Body $bodyJson `
                                -ContentType 'application/json' `
                        }
                        catch {
                            throw "##[error] Failed to update Kusto parameters in workspace ${workspaceId}: $($_.Exception.Message)"
                        }
                        
                    }

                    try {
                        # Bind Dataset to Cloud Connection
                        $datasourcesUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/datasources"                          
                        $datasources = Invoke-PowerBIRestMethod -Url $datasourcesUrl -Method Get | ConvertFrom-Json                           

                        if ($workspaceType -eq "operations") {
                            $extDatasource = $existingConnections | Where-Object { $_.DatasourceType -eq "Extension" }
                            $gatewayId = $extDatasource.gatewayId
                                                        
                            $allConnectionIds = $connectionIds + $kqlConnectionId

                            Invoke-PowerBIRestMethod -Url "groups/$workspaceId/datasets/$datasetId/Default.BindToGateway" -Method Post -Body (@{
                                "gatewayObjectId" = $gatewayId
                                "datasourceObjectIds" = $allConnectionIds
                            } | ConvertTo-Json -Depth 10)
                        }
                        if ($workspaceType -eq "reporting") {
                            $reportBody = @{
                                gatewayObjectId      = $connectionIds[0]
                                datasourceObjectIds  = $connectionIds
                            }
                            Invoke-PowerBIRestMethod `
                                -Url "groups/$workspaceId/datasets/$datasetId/Default.BindToGateway" `
                                -Method Post `
                                -Body ($reportBody | ConvertTo-Json -Depth 10)
                        }
                    }
                    catch {
                        throw "##[error] Failed to bind dataset to cloud connection in workspace ${workspaceId}: $($_.Exception.Message)"
                    }


                    try {
                        # Refresh Dataset
                        Refresh-PowerBI-Datasets -WorkspaceId $workspaceId -datasetId $datasetId -WaitTillPowerBIDatasetRefresh $WaitTillPowerBIDatasetRefresh
                    }
                    catch {
                        throw "##[error] Error refreshing dataset in workspace ${workspaceId}: $($_.Exception.Message)"
                    }
                    
                }
            }
            catch {
                $errorMessage += "##[error] Error processing workspace ${workspaceId}: $($_.Exception.Message)`n"
            }
        }
        if ($errorMessage) {
            throw $errorMessage 
        }
    }
    catch {
        throw "##[error] Error processing orchestration of POWERBI deployment: $($_.Exception.Message)"
    }
    
}
# Invoke the Orchestrator function
try {
    Invoke-ReportDeployment    
}
catch {
    throw "An error occurred while orchestrating POWERBI Deployment: $($_.Exception.Message)"
}