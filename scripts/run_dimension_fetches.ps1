# Run dimension table fetches (entity, park hours, events, metatable from S3; build dimdategroupid, dimseason locally).
# Invoked by ThemeParkDimensionFetch_6am scheduled task; can also be run manually.
# Writes to output_base/dimension_tables and output_base/logs. output_base from config/config.json or default.

$ErrorActionPreference = "Stop"
$ProjectRoot = "d:\GitHub\hazeydata\theme-park-crowd-report"
$PythonExe   = "C:\Python314\python.exe"

# Output base: config/config.json if present, else shared default (same as ETL)
$DefaultOutputBase = "D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report"
$OutputBase = $DefaultOutputBase
$CfgPath = Join-Path $ProjectRoot "config\config.json"
if (Test-Path $CfgPath) {
    try {
        $cfg = Get-Content $CfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($cfg.output_base) { $OutputBase = $cfg.output_base }
    } catch { }
}

Set-Location $ProjectRoot

$Scripts = @(
    "src/get_entity_table_from_s3.py",
    "src/get_park_hours_from_s3.py",
    "src/get_events_from_s3.py",
    "src/get_metatable_from_s3.py",
    "src/build_dimdategroupid.py",
    "src/build_dimseason.py"
)

foreach ($Script in $Scripts) {
    Write-Host "Running $Script ..."
    & $PythonExe $Script --output-base $OutputBase
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Dimension step failed: $Script (exit $LASTEXITCODE)"
        exit $LASTEXITCODE
    }
}

Write-Host "All dimension fetches and builds completed."
