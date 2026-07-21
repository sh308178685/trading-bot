$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$credentialConfigPath = 'C:\Users\SongHao\Downloads\weex-pro-trader\config\config.json'

if (-not (Test-Path -LiteralPath $credentialConfigPath)) {
    throw "WEEX credential config not found: $credentialConfigPath"
}

$credentials = Get-Content -Raw -LiteralPath $credentialConfigPath | ConvertFrom-Json
$required = @('apiKey', 'secretKey', 'passphrase')
foreach ($name in $required) {
    $value = [string]$credentials.$name
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "WEEX credential field is missing: $name"
    }
}

# Keep real credentials in this child process only. Never copy them into this project.
$env:WEEX_API_KEY = [string]$credentials.apiKey
$env:WEEX_SECRET_KEY = [string]$credentials.secretKey
$env:WEEX_PASSPHRASE = [string]$credentials.passphrase
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

Set-Location -LiteralPath $projectRoot
& (Join-Path $projectRoot 'start-martin.bat')
exit $LASTEXITCODE
