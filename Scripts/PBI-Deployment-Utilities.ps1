function Is-ConnectionPreExisting {
    <#
    .SYNOPSIS
    Checks if a Fabric connection with the given name exists.

    .PARAMETER AccessToken
    OAuth2 access token.

    .PARAMETER ConnectionName
    Name of the connection to check.

    .OUTPUTS
    Returns connection ID if exists, otherwise null.
    #>

    param(
        [string]$AccessToken,
        [string]$ConnectionName
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken" }
    $url = "https://api.fabric.microsoft.com/v1/connections"

    try {
        $connections = (Invoke-RestMethod -Method Get -Uri $url -Headers $headers).value

        $match = $connections | Where-Object { $_.displayName -ieq $ConnectionName } | Select-Object -First 1

        if ($match) {
            return $match.id
        } 
        return $null
        
    }
    catch {
        throw "Error fetching connections: $_"
    }
}

function New-FabricKQLCloudConnection {
        <#
    .SYNOPSIS
    Creates a new Fabric Cloud Connection if it doesn't already exist.

    .PARAMETER AccessToken
    OAuth 2.0 token used for API authentication.

    .PARAMETER ConnectionDisplayName
    Display name for the Fabric Cloud Connection.

    .PARAMETER SqlConnectionString
    SQL connection string containing server and database info.

    .RETURNS
    Returns the ID of the existing or newly created Cloud Connection.
    #>
    param(
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,

        [Parameter(Mandatory=$true)]
        [string]$ConnectionDisplayName,

        [Parameter(Mandatory=$true)]
        [string]$KustoUrl,

        [Parameter(Mandatory=$true)]
        [string]$KustoDatabase
    )

    try {
        # Step 1: Check for existing Connection 
        $existingId = Is-ConnectionPreExisting -AccessToken $AccessToken -ConnectionName $ConnectionDisplayName
        if ($existingId) {
            return $existingId
        } 
        else {
            if (-not $KustoUrl -or -not $KustoDatabase) {
                throw "Invalid connection string. Server and Database are required."
            }

            # Step 3: Define Cloud Connection API Details
            $createCloudConnectionUrl = "https://api.fabric.microsoft.com/v1/connections"
            $headers = @{
                "Content-Type"  = "application/json"
                "Authorization" = "Bearer $AccessToken"
            }

            # Step 4: Create JSON Payload
            $cloudConnectionPayload = @{
                connectivityType = "ShareableCloud"
                displayName      = $ConnectionDisplayName
                connectionDetails = @{
                    type           = "AzureDataExplorer"
                    creationMethod = "AzureDataExplorer.Contents"
                    parameters     = [System.Collections.Generic.List[Object]]@(
                        @{
                            dataType = "Text"
                            name     = "cluster"
                            value    = $KustoUrl
                        },
                        @{
                            dataType = "Text"
                            name     = "database"
                            value    = $KustoDatabase
                        }
                    )
                }
                privacyLevel = "Organizational"
                credentialDetails = @{
                    singleSignOnType = "None"
                    connectionEncryption = "Encrypted"
                    credentials = @{
                        credentialType            = "ServicePrincipal"
                        servicePrincipalClientId  = $ClientId
                        servicePrincipalSecret    = $ClientSecret
                        tenantId                  = $TenantId
                    }
                }
            } | ConvertTo-Json -Depth 10 -Compress

            # Step 5: Invoke API to Create Cloud Connection
            $createResponse = Invoke-RestMethod -Method Post -Uri $createCloudConnectionUrl -Headers $headers -Body $cloudConnectionPayload

            if ($null -eq $createResponse.id) {
                throw "Cloud Connection creation failed: No Connection ID returned."
            }

            return $createResponse.id
        }
    }
    catch {
        throw "An Error occurred while creating KQL cloud connection for: $($_.Exception.Message)"
    }
}

function New-FabricSQLCloudConnection {
        <#
    .SYNOPSIS
    Creates a new Fabric Cloud Connection if it doesn't already exist.

    .PARAMETER AccessToken
    OAuth 2.0 token used for API authentication.

    .PARAMETER ConnectionDisplayName
    Display name for the Fabric Cloud Connection.

    .PARAMETER SqlConnectionString
    SQL connection string containing server and database info.

    .RETURNS
    Returns the ID of the existing or newly created Cloud Connection.
    #>
    param(
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,

        [Parameter(Mandatory=$true)]
        [string]$ConnectionDisplayName,

        [Parameter(Mandatory=$true)]
        [string]$SqlConnectionString
    )

    try {
        # Step 1: Check for existing Connection 
        $existingId = Is-ConnectionPreExisting -AccessToken $AccessToken -ConnectionName $ConnectionDisplayName
        if ($existingId) {
            return $existingId
        } 
        else {
            # Step 2: Parse Connection String
            $connectionStringParts = $SqlConnectionString.Split(';') | ForEach-Object {
                if ($_ -match "^(.*)=(.*)$") {
                    @{ Key = $matches[1].Trim(); Value = $matches[2].Trim() }
                }
            }

            $server = ($connectionStringParts | Where-Object { $_.Key -match "Server|Data Source" }).Value
            $database = ($connectionStringParts | Where-Object { $_.Key -match "Database|Initial Catalog" }).Value

            if (-not $server -or -not $database) {
                throw "Invalid connection string. Server and Database are required."
            }


            # Step 3: Define Cloud Connection API Details
            $createCloudConnectionUrl = "https://api.fabric.microsoft.com/v1/connections"
            $headers = @{
                "Content-Type"  = "application/json"
                "Authorization" = "Bearer $AccessToken"
            }

            # Step 4: Create JSON Payload
            $cloudConnectionPayload = @{
                connectivityType = "ShareableCloud"
                displayName      = $ConnectionDisplayName
                connectionDetails = @{
                    type           = "SQL"
                    creationMethod = "SQL"
                    parameters     = [System.Collections.Generic.List[Object]]@(
                        @{
                            dataType = "Text"
                            name     = "server"
                            value    = $server
                        },
                        @{
                            dataType = "Text"
                            name     = "database"
                            value    = $database
                        }
                    )
                }
                privacyLevel = "Organizational"
                credentialDetails = @{
                    singleSignOnType = "None"
                    connectionEncryption = "Encrypted"
                    credentials = @{
                        credentialType            = "ServicePrincipal"
                        servicePrincipalClientId  = $ClientId
                        servicePrincipalSecret    = $ClientSecret
                        tenantId                  = $TenantId
                    }
                }
            } | ConvertTo-Json -Depth 10 -Compress

            # Step 5: Invoke API to Create Cloud Connection
            $createResponse = Invoke-RestMethod -Method Post -Uri $createCloudConnectionUrl -Headers $headers -Body $cloudConnectionPayload

            if ($null -eq $createResponse.id) {
                throw "Cloud Connection creation failed: No Connection ID returned."
            }

            return $createResponse.id
        }
    }
    catch {
        throw "An Error occurred while creating cloud connection for: $($_.Exception.Message)"
    }
}

function Refresh-PowerBI-Datasets {
    <#
    .SYNOPSIS
        Refresh PowerBI Datasets.
    
    .DESCRIPTION
        This function refreshes PowerBI datasets within a given workspace.
    
    .PARAMETER WorkspaceId 
        The ID of the PowerBI workspace.
    
    .PARAMETER WaitTillPowerBIDatasetRefresh
        Boolean value indicating whether to wait until the dataset refresh is complete.
    
    .PARAMETER DeployedReports
        List of dataset names to refresh.
    
    .INPUTS
        String
    
    .OUTPUTS
        Object
    #>
    
    param (
        [Parameter(Mandatory)]
        [string] $WorkspaceId,
        [Parameter(Mandatory)]
        [string] $datasetId,
        [Parameter(Mandatory)]
        [string] $WaitTillPowerBIDatasetRefresh
    )

    $response = $null
    $secondsDelay = 30
    $retries = 60

    try {
        $datasetRefreshUrl = "groups/$WorkspaceId/datasets/$datasetId/refreshes"
        
        $refreshResponse = Get-PowerBI-DatasetRefresh-Status -WorkspaceId $WorkspaceId -DatasetId $datasetId
        # Only convert to JSON if the response is a string.
        if ($refreshResponse -is [string]) {
            $responseData = $refreshResponse | ConvertFrom-Json
        }
        else {
            $responseData = $refreshResponse
        }
    
        $refreshStatus = $responseData.status
        $startTime = (Get-Date).ToUniversalTime()

        if ($refreshStatus -eq 'Unknown' -or $startTime -ge $responseData.startTime) {
            Invoke-PowerBIRestMethod -Method Post -Url $datasetRefreshUrl -WarningAction Ignore
            Start-Sleep $secondsDelay                 
        } else {                    
            continue
        }

        if ($WaitTillPowerBIDatasetRefresh -eq 'True') {
            $retryCount = 0
            $completed = $false
            
            while (-not $completed -and $retryCount -lt $retries) {
                Start-Sleep $secondsDelay
                $refreshStatus = Get-PowerBI-DatasetRefresh-Status -WorkspaceId $WorkspaceId -DatasetId $datasetId                      

                if ($refreshStatus.status -eq 'Completed') {
                    $completed = $true                                                 
                } elseif ($refreshStatus.status -eq 'Failed') {                                                     
                    throw "Dataset refresh failed."
                } else {
                    $retryCount++
                }

                if ($retryCount -ge $retries) {                           
                    throw "Dataset refresh exceeded max retries."
                }
            }
        }

    }
    catch {
        throw "Error while refreshing dataset ${datasetId}"
    }
    return $response
}

function Get-PowerBI-DatasetRefresh-Status {
    <#
    .SYNOPSIS
        Get PowerBI Dataset Refresh Status.
    
    .DESCRIPTION
        This function retrieves the latest refresh status of a PowerBI dataset.

    .PARAMETER WorkspaceId 
        The ID of the PowerBI workspace.
    
    .PARAMETER DatasetId 
        The ID of the dataset.
    
    .INPUTS
        String
    
    .OUTPUTS
        String
    #>
    
    param (
        [Parameter(Mandatory)]
        [string] $WorkspaceId,
        [Parameter(Mandatory)]
        [string] $DatasetId  
    )

    try {
        $datasetRefreshStatusUrl = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$DatasetId/refreshes?top=1"
        $refreshResponse = Invoke-PowerBIRestMethod -Method Get -Url $datasetRefreshStatusUrl -WarningAction Ignore
        $responseData = $refreshResponse | ConvertFrom-Json
    
        $latestRefresh = $responseData.value[0]
    
        if ($latestRefresh) {
            return [PSCustomObject]@{
                Status    = $latestRefresh.status
                StartTime = $latestRefresh.startTime
                EndTime   = $latestRefresh.endTime
            }
        }
        else {
            return [PSCustomObject]@{
                Status    = "Unknown"
                StartTime = $null
                EndTime   = $null
            }
        }
    }
    catch {
        throw "No refresh history found for dataset ID: $DatasetId"
    }
}

function Create-PowerBI-Report {
    <#
    .SYNOPSIS
    Deploys a Power BI report to the specified workspace, deleting existing reports before deployment.

    .PARAMETER AccessToken
    OAuth 2.0 token used for API authentication.

    .PARAMETER PowerBIReportFilePath
    File path of the Power BI report (.pbix) to deploy.

    .PARAMETER PowerBIReportName
    Display name for the deployed Power BI report.

    .PARAMETER WorkspaceId
    Workspace ID where the report will be deployed.

    .RETURNS
    Returns the response object from the report deployment API.
    #>
    param (
        [Parameter(Mandatory)]
        [string] $AccessToken,
        [Parameter(Mandatory)]
        [string] $PowerBIReportFilePath,
        [Parameter(Mandatory)]
        [string] $PowerBIReportName,
        [Parameter(Mandatory)]
        [string] $WorkspaceId
    )

    $response = $null
    $retrycount = 0
    $completed = $false
    $retries = 5
    $secondsDelay = 10
        
    try {          
        $headers = @{ "Authorization" = "Bearer $AccessToken" }
        
        $reports = Invoke-RestMethod -Headers $headers -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports"

        if ($reports -and $reports.value) {
            foreach ($r in $reports.value) {
                if ($PowerBIReportName -eq $r.name) {
                    Invoke-RestMethod -Method Delete -Headers $headers -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports/$($r.id)"
                    break  
                }
            }
        }
        
        while (-not $completed) {
            try {
               
                $response = New-PowerBIReport -Path $PowerBIReportFilePath -Name $PowerBIReportName -WorkspaceId $WorkspaceId -ConflictAction CreateOrOverwrite -Timeout 300

                $completed = $true
            }
            catch {
                if ($retrycount -ge $retries) {
                    throw ("Report deployment [{0}] failed after {1} retries." -f $PowerBIReportName, $retrycount)
                } else {
                   
                    Start-Sleep -Seconds $secondsDelay
                    $retrycount++
                }
            }
        }
    }
    catch {
        throw "Error while deploying Power BI Report: $($_.Exception.Message)"
    }
    
    return $response
}

function Write-ArtifactGuids {
    <#
    .SYNOPSIS
    Writes artifact GUIDs to a JSON file and triggers a Python script to update OneLake metadata.

    .DESCRIPTION
    This function serializes a hashtable of artifact GUIDs into JSON and invokes a Python script
    to update OneLake metadata. It ensures consistent formatting and passes required credentials
    and workspace information to the script.

    .PARAMETER PythonScriptPath
    Path to the Python script for updating OneLake metadata. Defaults to "DevOps/COMMON/update_onelake_metadata.py".

    .PARAMETER TenantId
    Tenant ID for authentication.

    .PARAMETER ClientId
    Client ID for authentication.

    .PARAMETER ClientSecret
    Client Secret (password) for authentication.

    .PARAMETER OperationsWorkspaceName
    Name of the operations workspace.

    .PARAMETER OperationsWorkspaceId
    ID of the operations workspace.

    .PARAMETER WorkspaceName
    Name of the current workspace.

    .PARAMETER WorkspaceId
    ID of the current workspace.
    #>

    param (
        [string]$PythonScriptPath = "DevOps/COMMON/update_onelake_metadata.py",

        [Parameter(Mandatory = $true)]
        [string]$TenantId,

        [Parameter(Mandatory = $true)]
        [string]$ClientId,

        [Parameter(Mandatory = $true)]
        [string]$ClientSecret,

        [Parameter(Mandatory = $true)]
        [string]$OperationsWorkspaceName,

        [Parameter(Mandatory = $true)]
        [string]$OperationsWorkspaceId,

        [Parameter(Mandatory = $true)]
        [string]$WorkspaceName,

        [Parameter(Mandatory = $true)]
        [string]$WorkspaceId
    )

    try {
        # Build argument list
        $arguments = @(
            "`"$PythonScriptPath`"",
            "`"$TenantId`"",
            "`"$ClientId`"",
            "`"$ClientSecret`"",
            "`"$OperationsWorkspaceName`"",
            "`"$OperationsWorkspaceId`"",
            "`"$WorkspaceName`"",
            "`"$WorkspaceId`""
        )

        $command = "python " + ($arguments -join " ")
        Invoke-Expression $command

        if ($LASTEXITCODE -ne 0) {
            throw "Failed to execute update_onelake_metadata.py. Exit code: $LASTEXITCODE"
        }
    }
    catch {
        throw "Error in Write-ArtifactGuids: $_"
    }
}