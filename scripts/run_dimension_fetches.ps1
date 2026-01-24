# Run dimension table fetches (entity, park hours, events, metatable from S3; build dimdategroupid, dimseason locally).
# Invoked by ThemeParkDimensionFetch_6am scheduled task; can also be run manually.
# Writes to output/dimension_tables (and output/logs) under project root.

$ErrorActionPreference = "Stop"
$ProjectRoot = "d:\GitHub\hazeydata\theme-park-crowd-report"
$OutputBase  = Join-Path $ProjectRoot "output"
$PythonExe   = "C:\Python314\python.exe"

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
