[CmdletBinding()]
param(
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

$project = 'amazon-stage7-clean'
$compose = @('-p', $project, '-f', 'compose.yaml', '-f', 'compose.stage7-clean.yaml')
$outDir = Join-Path $PWD 'data\work\stage7\clean_reproduction'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

function Wait-AirflowRun {
    param([string]$DagId, [string]$RunId, [int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $state = (docker compose @compose exec -T postgres psql -U postgres -d airflow `
            -X -A -t -c "SELECT state FROM dag_run WHERE dag_id='$DagId' AND run_id='$RunId';").Trim()
        if ($state -in @('success','failed')) { return $state }
        if ((Get-Date) -ge $deadline) { throw "Airflow run did not finish: $DagId $RunId state=$state" }
        Start-Sleep -Seconds 5
    } while ($true)
}

docker compose @compose config --quiet
docker compose @compose up -d postgres
docker compose @compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -f /opt/project/sql/017_collection_review_semantics.sql | Out-Null
docker compose @compose run --rm airflow-init
docker compose @compose up -d airflow-api-server airflow-scheduler airflow-dag-processor

$deadline = (Get-Date).AddMinutes(5)
do {
    $health = docker compose @compose ps --format json
    if ($health -match 'healthy') { break }
    if ((Get-Date) -ge $deadline) { throw 'Clean Airflow stack did not become healthy within five minutes.' }
    Start-Sleep -Seconds 10
} while ($true)

docker compose @compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -c "SELECT to_regclass('meta.stage6_candidate_scope'),to_regclass('evaluation.dataset'),to_regclass('integration.source_record_impact'),to_regclass('target.product_movie_review_attribution');" |
    Set-Content -LiteralPath (Join-Path $outDir 'migration_chain.txt') -Encoding utf8

docker compose @compose exec -T airflow-scheduler airflow dags list-import-errors |
    Set-Content -LiteralPath (Join-Path $outDir 'airflow_import_errors.txt') -Encoding utf8

$runId = 'manual__stage7_clean_' + (Get-Date -Format 'yyyyMMddTHHmmss')
docker compose @compose exec -T airflow-scheduler airflow dags unpause stage0_smoke | Out-Null
docker compose @compose exec -T airflow-scheduler airflow dags trigger stage0_smoke `
    --run-id $runId --conf '{"batch_id":"stage7-clean-smoke"}' |
    Set-Content -LiteralPath (Join-Path $outDir 'smoke_trigger.txt') -Encoding utf8
$smokeState = Wait-AirflowRun -DagId 'stage0_smoke' -RunId $runId
docker compose @compose exec -T airflow-scheduler airflow tasks states-for-dag-run stage0_smoke $runId |
    Set-Content -LiteralPath (Join-Path $outDir 'smoke_states.txt') -Encoding utf8
if ($smokeState -ne 'success') { throw "Clean smoke run ended as $smokeState" }

$sampleRunId = 'manual__stage7_clean_sample_' + (Get-Date -Format 'yyyyMMddTHHmmss')
docker compose @compose exec -T airflow-scheduler airflow dags unpause stage1_review_sample_ingest | Out-Null
docker compose @compose exec -T airflow-scheduler airflow dags trigger stage1_review_sample_ingest `
    --run-id $sampleRunId `
    --conf '{"batch_id":"stage7-clean-sample","sample_limit":10,"batch_size":5,"parser_version":"snap-movies-review-v3","scope_start_record":1,"load_strategy":"copy","verify_retry_after_commit":false}' |
    Set-Content -LiteralPath (Join-Path $outDir 'sample_trigger.txt') -Encoding utf8
$sampleState = Wait-AirflowRun -DagId 'stage1_review_sample_ingest' -RunId $sampleRunId
docker compose @compose exec -T airflow-scheduler airflow tasks states-for-dag-run stage1_review_sample_ingest $sampleRunId |
    Set-Content -LiteralPath (Join-Path $outDir 'sample_states.txt') -Encoding utf8
if ($sampleState -ne 'success') { throw "Clean sample run ended as $sampleState" }
docker compose @compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -c "SELECT parser_version,parse_status,count(*) FROM staging.review_parse_result GROUP BY parser_version,parse_status ORDER BY parse_status;" |
    Set-Content -LiteralPath (Join-Path $outDir 'sample_database_result.txt') -Encoding utf8

docker compose @compose ps --format json |
    Set-Content -LiteralPath (Join-Path $outDir 'container_status.jsonl') -Encoding utf8
$manifestPath = Join-Path $outDir 'sha256_manifest.csv'
Get-ChildItem -LiteralPath $outDir -File | Where-Object FullName -ne $manifestPath | Get-FileHash -Algorithm SHA256 |
    Select-Object Path, Hash | Export-Csv -NoTypeInformation -Encoding utf8 `
    $manifestPath

if (-not $KeepRunning) {
    docker compose @compose stop
}

Get-ChildItem -LiteralPath $outDir -File | Select-Object Name, Length, LastWriteTime

