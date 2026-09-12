$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $ProjectRoot 'web')
try {
    npm ci
    npm run build
}
finally {
    Pop-Location
}

Push-Location $ProjectRoot
try {
    cargo build --locked --release
}
finally {
    Pop-Location
}

Write-Host "Built: $ProjectRoot\target\release\nva2dlna.exe"

