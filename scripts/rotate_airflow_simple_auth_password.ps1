[CmdletBinding()]
param(
    [string]$PasswordFile = (Join-Path $PSScriptRoot '..\airflow\logs\simple_auth_manager_passwords.json.generated')
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $PasswordFile)) {
    throw "Airflow Simple Auth Manager password file was not found: $PasswordFile"
}

$passwordBytes = New-Object byte[] 24
[Security.Cryptography.RandomNumberGenerator]::Fill($passwordBytes)
$password = [Convert]::ToBase64String($passwordBytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')

$document = Get-Content -LiteralPath $PasswordFile -Raw | ConvertFrom-Json
if (-not $document.PSObject.Properties['admin']) {
    throw 'The expected admin account is absent from the password file.'
}

$document.admin = $password
$json = $document | ConvertTo-Json -Compress
[IO.File]::WriteAllText((Resolve-Path -LiteralPath $PasswordFile), $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))

Write-Output 'Airflow local admin password rotated; the new value was not displayed.'
