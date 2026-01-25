# Run the Queue-Times.com wait time fetcher in a loop: fetch, write to staging/queue_times, sleep, repeat.
# The scraper writes to staging only; the morning ETL merges yesterday's staging into fact_tables/clean.
# Staging is also available for live use (e.g. Twitch/YouTube). Stop with Ctrl+C.
#
# Optional: Register a Windows scheduled task "At log on" or "At startup" that runs this script
# so the loop restarts after reboot.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1 -IntervalSeconds 900 -OutputBase "D:\Path\output"

param(
    [int]   $IntervalSeconds = 600,   # 10 minutes between fetches
    [string] $OutputBase     = ""     # default: (ProjectRoot)/output
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe   = "python"
if ($OutputBase -eq "") {
    $OutputBase = Join-Path $ProjectRoot "output"
}

Set-Location $ProjectRoot

Write-Host "Queue-Times loop: interval=$IntervalSeconds s, output=$OutputBase"
Write-Host "Stop with Ctrl+C."
& $PythonExe src/get_wait_times_from_queue_times.py --output-base $OutputBase --interval $IntervalSeconds
