[CmdletBinding()]
param(
    [string]$OutputDirectory = 'submission\amazon_movies_etl_final_20261107'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $projectRoot
$destination = [IO.Path]::GetFullPath((Join-Path $projectRoot $OutputDirectory))
if (Test-Path -LiteralPath $destination) {
    throw "Submission directory already exists; choose a new OutputDirectory: $destination"
}
New-Item -ItemType Directory -Path $destination -Force | Out-Null

function Copy-SubmissionFile([string]$Source, [string]$TargetRelative) {
    if (-not (Test-Path -LiteralPath $Source -PathType Leaf)) {
        throw "Required submission file is missing: $Source"
    }
    $target = Join-Path $destination $TargetRelative
    New-Item -ItemType Directory -Path (Split-Path $target -Parent) -Force | Out-Null
    Copy-Item -LiteralPath $Source -Destination $target
}

function Copy-SubmissionTree([string]$SourceDirectory, [string]$TargetRelative) {
    if (-not (Test-Path -LiteralPath $SourceDirectory -PathType Container)) {
        throw "Required submission directory is missing: $SourceDirectory"
    }
    Get-ChildItem -LiteralPath $SourceDirectory -Recurse -File | Where-Object {
        $_.Extension -notin @('.pyc', '.pyo') -and $_.FullName -notmatch '[\\/]__pycache__[\\/]'
    } | ForEach-Object {
        $relative = $_.FullName.Substring([IO.Path]::GetFullPath($SourceDirectory).Length).TrimStart([char]92, [char]47)
        Copy-SubmissionFile $_.FullName (Join-Path $TargetRelative $relative)
    }
}

foreach ($file in @('README.md','.gitignore','.env.example','pyproject.toml','compose.yaml','compose.stage7-clean.yaml')) {
    Copy-SubmissionFile (Join-Path $projectRoot $file) $file
}
foreach ($tree in @('docs','src','airflow\dags','sql','requirements','docker','scripts','config','tests')) {
    Copy-SubmissionTree (Join-Path $projectRoot $tree) $tree
}

$selectedEvidence = [ordered]@{
    'data\work\stage2\product_list.tsv' = 'evidence\stage2\product_list.tsv'
    'data\work\stage2\anomaly_summary.tsv' = 'evidence\stage2\anomaly_summary.tsv'
    'data\work\stage2\malformed_blocks.tsv' = 'evidence\stage2\malformed_blocks.tsv'
    'data\work\stage6\formal_evaluation\movie\review_material.html' = 'evidence\evaluation\movie\review_material.html'
    'data\work\stage6\formal_evaluation\movie\review_manifest.json' = 'evidence\evaluation\movie\review_manifest.json'
    'data\work\stage6\formal_evaluation\movie\imports\answers_canonical_20261105.tsv' = 'evidence\evaluation\movie\answers_canonical.tsv'
    'data\work\stage6\formal_evaluation\person\review_material.html' = 'evidence\evaluation\person\review_material.html'
    'data\work\stage6\formal_evaluation\person\review_manifest.json' = 'evidence\evaluation\person\review_manifest.json'
    'data\work\stage7\evaluation\person\answers_canonical_20261106.tsv' = 'evidence\evaluation\person\answers_canonical.tsv'
    'data\work\stage7\evaluation\person\answer_import_manifest.json' = 'evidence\evaluation\person\answer_import_manifest.json'
    'data\work\stage7\evaluation\database_acceptance.json' = 'evidence\evaluation\database_acceptance.json'
    'data\work\stage7\evaluation\results\evaluation_metrics.json' = 'evidence\evaluation\results\evaluation_metrics.json'
    'data\work\stage7\evaluation\results\movie_frozen_predictions_and_labels.tsv' = 'evidence\evaluation\results\movie_frozen_predictions_and_labels.tsv'
    'data\work\stage7\evaluation\results\person_frozen_predictions_and_labels.tsv' = 'evidence\evaluation\results\person_frozen_predictions_and_labels.tsv'
    'data\work\stage7\evaluation\results\person_name_composition.tsv' = 'evidence\evaluation\results\person_name_composition.tsv'
    'data\work\stage7\evaluation\results\representative_errors.tsv' = 'evidence\evaluation\results\representative_errors.tsv'
    'data\work\stage7\case_inventory\case_inventory.tsv' = 'evidence\cases\case_inventory.tsv'
    'data\work\stage7\case_inventory\case_inventory.json' = 'evidence\cases\case_inventory.json'
    'data\work\stage7\case_inventory\cross_version_verification.json' = 'evidence\cases\cross_version_verification.json'
    'data\work\stage7\collection_demo\john_wayne_collection.json' = 'evidence\collection\john_wayne_collection.json'
    'data\work\task8\small_sample_acceptance.json' = 'evidence\task8\small_sample_acceptance.json'
    'data\work\task8\small_sample_database_validation.txt' = 'evidence\task8\small_sample_database_validation.txt'
    'data\work\task8\full_status.json' = 'evidence\task8\full_status.json'
    'data\work\task8\full_validation.txt' = 'evidence\task8\full_validation.txt'
    'data\work\lineage_audit\full_status.json' = 'evidence\lineage\full_status.json'
    'data\work\lineage_audit\coverage_summary.csv' = 'evidence\lineage\coverage_summary.csv'
    'data\work\lineage_audit\gap_detail.csv' = 'evidence\lineage\gap_detail.csv'
    'data\work\lineage_audit\repair_history.csv' = 'evidence\lineage\repair_history.csv'
    'data\work\lineage_audit\impact_demo.json' = 'evidence\lineage\impact_demo.json'
    'data\work\lineage_audit\full_validation.txt' = 'evidence\lineage\full_validation.txt'
    'data\work\lineage_audit\lineage_queries_revalidation.txt' = 'evidence\lineage\lineage_queries_revalidation.txt'
    'data\work\lineage_audit\business_queries_revalidation.txt' = 'evidence\lineage\business_queries_revalidation.txt'
}
foreach ($entry in $selectedEvidence.GetEnumerator()) {
    Copy-SubmissionFile (Join-Path $projectRoot $entry.Key) $entry.Value
}
Copy-SubmissionTree (Join-Path $projectRoot 'data\work\stage7\evidence') 'evidence\reproduction\current_environment'
Copy-SubmissionTree (Join-Path $projectRoot 'data\work\stage7\clean_reproduction') 'evidence\reproduction\clean_environment'

$forbidden = Get-ChildItem -LiteralPath $destination -Recurse -File -Force | Where-Object {
    $_.Name -eq '.env' -or $_.Extension -in @('.gz','.vhdx','.bak','.dump','.pem','.key','.parquet','.sqlite','.db') -or
    $_.FullName -match '[\\/](raw|snapshots|backups|logs|pg_wal)[\\/]'
}
if ($forbidden) {
    throw "Forbidden files entered submission package: $($forbidden.FullName -join ', ')"
}

$textExtensions = @('.py','.sql','.md','.yaml','.yml','.toml','.ps1','.sh','.json','.txt','.tsv','.csv','.html','.example')
$secretPattern = '(?i)(api[_-]?key|access[_-]?token|password|secret)\s*[:=]\s*["''][^"'']{8,}["'']'
$secretHits = foreach ($file in Get-ChildItem -LiteralPath $destination -Recurse -File) {
    if ($textExtensions -contains $file.Extension.ToLowerInvariant() -or $file.Name -eq '.env.example') {
        Select-String -LiteralPath $file.FullName -Pattern $secretPattern -ErrorAction SilentlyContinue | Where-Object {
            $_.Line -notmatch '\$\{?[A-Z][A-Z0-9_]*\}?'
        } | ForEach-Object {
            [pscustomobject]@{ File = $file.FullName.Substring($destination.Length + 1); Line = $_.LineNumber }
        }
    }
}
if ($secretHits) {
    throw "Possible embedded secrets require review: $($secretHits | ConvertTo-Json -Compress)"
}

$payloadFiles = @(Get-ChildItem -LiteralPath $destination -Recurse -File)
$audit = [ordered]@{
    generated_at = (Get-Date).ToString('o')
    output_directory = $destination
    payload_file_count = $payloadFiles.Count
    payload_total_bytes = ($payloadFiles | Measure-Object Length -Sum).Sum
    expected_final_file_count = $payloadFiles.Count + 2
    forbidden_file_count = @($forbidden).Count
    possible_secret_count = @($secretHits).Count
    git_history_required = $false
    page_collection = [ordered]@{ attempts = 5; successes = 0; scope = 'bounded_realtime_validation' }
    offline_ucsd = [ordered]@{ source_records = 203766; covered_products = 73813; target_products = 253059 }
}
$audit | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $destination 'PACKAGE_AUDIT.json') -Encoding utf8

$manifestPath = Join-Path $destination 'MANIFEST.sha256.csv'
Get-ChildItem -LiteralPath $destination -Recurse -File | Where-Object FullName -ne $manifestPath | ForEach-Object {
    [pscustomobject]@{
        RelativePath = $_.FullName.Substring($destination.Length + 1).Replace('\\','/')
        SizeBytes = $_.Length
        Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
} | Sort-Object RelativePath | Export-Csv -LiteralPath $manifestPath -NoTypeInformation -Encoding utf8

$finalFiles = @(Get-ChildItem -LiteralPath $destination -Recurse -File)

[pscustomobject]@{
    SubmissionDirectory = $destination
    FileCount = $finalFiles.Count
    TotalBytes = ($finalFiles | Measure-Object Length -Sum).Sum
    ForbiddenFiles = 0
    PossibleSecrets = 0
}
