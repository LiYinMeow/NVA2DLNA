$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $ProjectRoot 'web')
try {
    # npm.cmd, not npm: the npm.ps1 shim reads $MyInvocation.Statement, which
    # StrictMode Latest turns into a terminating error.
    npm.cmd ci
    npm.cmd run build
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

