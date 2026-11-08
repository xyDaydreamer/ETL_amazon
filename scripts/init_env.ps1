[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$templatePath = Join-Path $projectRoot '.env.example'
$envPath = Join-Path $projectRoot '.env'

if (Test-Path -LiteralPath $envPath) {
    throw '.env already exists; refusing to overwrite it.'
}

function New-RandomBytes([int]$count) {
    $bytes = [byte[]]::new($count)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return $bytes
}

function New-HexSecret {
    return [Convert]::ToHexString((New-RandomBytes 32)).ToLowerInvariant()
}

function New-FernetKey {
    return [Convert]::ToBase64String((New-RandomBytes 32)).Replace('+', '-').Replace('/', '_')
}

$content = Get-Content -Raw -LiteralPath $templatePath
$replacements = [ordered]@{
    'change-me-superuser'       = New-HexSecret
    'change-me-airflow'         = New-HexSecret
    'change-me-movies'          = New-HexSecret
    'change-me-api-secret'      = New-HexSecret
    'change-me-jwt-secret'      = New-HexSecret
    'change-me-valid-fernet-key' = New-FernetKey
}

foreach ($item in $replacements.GetEnumerator()) {
    $content = $content.Replace($item.Key, $item.Value)
}

if ($content -match 'change-me-') {
    throw 'One or more secret placeholders were not replaced.'
}

[IO.File]::WriteAllText($envPath, $content, [Text.UTF8Encoding]::new($false))
Write-Output 'Generated .env with local random secrets; values were not displayed.'

