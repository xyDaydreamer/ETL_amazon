[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

$outDir = Join-Path $PWD 'data\work\stage7\submission_audit'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$includedExtensions = @('.py','.sql','.md','.yaml','.yml','.toml','.ps1','.sh','.json','.txt')
$excludedRoots = @(
    (Join-Path $PWD 'data\raw'), (Join-Path $PWD 'data\work'),
    (Join-Path $PWD 'airflow\logs'), (Join-Path $PWD 'backups')
)
$files = Get-ChildItem -LiteralPath $PWD -Recurse -File -Force | Where-Object {
    $path = $_.FullName
    $_.Name -ne '.env' -and
    $includedExtensions -contains $_.Extension.ToLowerInvariant() -and
    -not ($excludedRoots | Where-Object { $path.StartsWith($_, [StringComparison]::OrdinalIgnoreCase) })
}

$files | ForEach-Object {
    [pscustomobject]@{
        RelativePath = $_.FullName.Substring($PWD.Path.Length + 1)
        SizeBytes = $_.Length
        Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
} | Sort-Object RelativePath | Export-Csv -NoTypeInformation -Encoding utf8 `
    (Join-Path $outDir 'submission_file_manifest.csv')

$large = Get-ChildItem -LiteralPath $PWD -Recurse -File -Force | Where-Object Length -gt 50MB | ForEach-Object {
    [pscustomobject]@{
        RelativePath = $_.FullName.Substring($PWD.Path.Length + 1)
        SizeBytes = $_.Length
        ExpectedExcluded = $_.FullName -like "$($PWD.Path)\data\raw\*" -or
                           $_.FullName -like "$($PWD.Path)\data\work\*" -or
                           $_.FullName -like "$($PWD.Path)\backups\*" -or
                           $_.Extension -eq '.gz'
    }
}
$large | Sort-Object SizeBytes -Descending | Export-Csv -NoTypeInformation -Encoding utf8 `
    (Join-Path $outDir 'large_files.csv')

$pattern = '(?i)(api[_-]?key|token|password|secret)\s*[:=]\s*["''][^"'']{8,}["'']'
$hits = foreach ($file in $files) {
    Select-String -LiteralPath $file.FullName -Pattern $pattern -AllMatches -ErrorAction SilentlyContinue | ForEach-Object {
        [pscustomobject]@{
            RelativePath = $file.FullName.Substring($PWD.Path.Length + 1)
            LineNumber = $_.LineNumber
            Finding = 'possible_embedded_secret_review_required'
        }
    }
}
$hits | Export-Csv -NoTypeInformation -Encoding utf8 (Join-Path $outDir 'secret_scan_findings.csv')

[pscustomobject]@{
    GitMetadataPresent = Test-Path -LiteralPath (Join-Path $PWD '.git')
    CandidateFileCount = $files.Count
    LargeFileCount = @($large).Count
    PossibleSecretFindingCount = @($hits).Count
    CheckedAt = (Get-Date).ToString('o')
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $outDir 'audit_summary.json') -Encoding utf8

Get-Content -LiteralPath (Join-Path $outDir 'audit_summary.json')

