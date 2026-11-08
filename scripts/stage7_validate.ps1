[CmdletBinding()]
param(
    [string]$MovieId = '',
    [string]$FieldName = 'canonical_title',
    [string]$Asin = '',
    [string]$SourceReviewId = '',
    [string]$SourceRecordId = '',
    [string]$PersonId = ''
)

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

$outDir = Join-Path $PWD 'data\work\stage7\evidence'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

docker version --format '{{.Server.Version}}' | Out-Null
docker compose ps --format json | Set-Content -LiteralPath (Join-Path $outDir 'container_status.jsonl') -Encoding utf8

docker compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -f /opt/project/sql/016_stage7_query_views.sql |
    Set-Content -LiteralPath (Join-Path $outDir 'migration_016.txt') -Encoding utf8
docker compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -f /opt/project/sql/017_collection_review_semantics.sql |
    Set-Content -LiteralPath (Join-Path $outDir 'migration_017.txt') -Encoding utf8

$lineageArgs = @('-v', 'ON_ERROR_STOP=1')
foreach ($entry in @{
        movie_id = $MovieId; field_name = $FieldName; asin = $Asin
        source_review_id = $SourceReviewId; source_record_id = $SourceRecordId; person_id = $PersonId
    }.GetEnumerator()) {
    if ($entry.Value) { $lineageArgs += @('-v', "$($entry.Key)=$($entry.Value)") }
}
docker compose exec -T postgres psql -U postgres -d amazon_movies @lineageArgs `
    -f /opt/project/sql/queries/stage7_lineage_queries.sql |
    Set-Content -LiteralPath (Join-Path $outDir 'lineage_queries.txt') -Encoding utf8

$businessArgs = @('-v', 'ON_ERROR_STOP=1')
foreach ($entry in @{ movie_id = $MovieId; person_id = $PersonId; asin = $Asin }.GetEnumerator()) {
    if ($entry.Value) { $businessArgs += @('-v', "$($entry.Key)=$($entry.Value)") }
}
docker compose exec -T postgres psql -U postgres -d amazon_movies @businessArgs `
    -f /opt/project/sql/queries/stage7_business_queries.sql |
    Set-Content -LiteralPath (Join-Path $outDir 'business_queries.txt') -Encoding utf8

docker compose exec -T postgres psql -U postgres -d amazon_movies `
    -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage7_quality_validation.sql |
    Set-Content -LiteralPath (Join-Path $outDir 'quality_validation.txt') -Encoding utf8

docker compose exec -T airflow-scheduler airflow dags list-import-errors |
    Set-Content -LiteralPath (Join-Path $outDir 'airflow_import_errors.txt') -Encoding utf8
docker compose exec -T airflow-scheduler airflow dags list |
    Set-Content -LiteralPath (Join-Path $outDir 'airflow_dags.txt') -Encoding utf8

$manifestPath = Join-Path $outDir 'sha256_manifest.csv'
Get-ChildItem -LiteralPath $outDir -File | Where-Object FullName -ne $manifestPath | Get-FileHash -Algorithm SHA256 |
    Select-Object Path, Hash | Export-Csv -NoTypeInformation -Encoding utf8 `
    $manifestPath

Get-ChildItem -LiteralPath $outDir -File | Select-Object Name, Length, LastWriteTime

