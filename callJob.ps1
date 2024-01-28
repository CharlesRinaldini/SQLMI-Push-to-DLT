using namespace System.Net

param($Request, $TriggerMetadata)

$dbrAPI = 'https://adb-<yourDatabricksOrgId>.azuredatabricks.net/api/'
$apiHeaders = @{
    Authorization = "Bearer dapi<yourDatabricksAPIKey>"
}
$pipelineId = '<yourPipelineId>'

$jobCreateUri = $dbrAPI + "2.0/pipelines/$pipelineId/updates"

$jobCreateJson = '{ "full_refresh": "true" }'

$newJob = Invoke-RestMethod -Uri $jobCreateUri -Method POST -Headers $apiHeaders -Body $jobCreateJson -UseBasicParsing

$body = '{"content":"Created update with request_id: $($newJob.request_id)"}'

Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
    StatusCode = [HttpStatusCode]::OK
    Body = $body
})
