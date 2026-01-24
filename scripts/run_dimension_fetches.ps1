# Run dimension table fetches (entity, park hours, events) from S3.
# Invoked by ThemeParkDimensionFetch_6am scheduled task; can also be run manually.
# Uses default --output-base (Dropbox); same Python as ETL.

$ErrorActionPreference = "Stop"
$ProjectRoot = "d:\GitHub\hazeydata\theme-park-crowd-report"
$PythonExe   = "C:\Python314\python.exe"

Set-Location $ProjectRoot

$Scripts = @(
    "src/get_entity_table_from_s3.py",
    "src/get_park_hours_from_s3.py",
    "src/get_events_from_s3.py"
)

foreach ($Script in $Scripts) {
    Write-Host "Running $Script ..."
    & $PythonExe $Script
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Dimension fetch failed: $Script (exit $LASTEXITCODE)"
        exit $LASTEXITCODE
    }
}

Write-Host "All dimension fetches completed."
