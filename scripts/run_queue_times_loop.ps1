# Run the Queue-Times.com wait time fetcher in a loop: fetch, write to staging/queue_times, sleep, repeat.
# The scraper writes to staging only; the morning ETL merges yesterday's staging into fact_tables/clean.
# By default uses dimparkhours to only call the API when a park is in-window (open-90 to close+90 in park TZ).
# Staging is also available for live use (e.g. Twitch/YouTube). Stop with Ctrl+C.
#
# ThemeParkQueueTimes_5min (from register_scheduled_tasks.ps1) runs this script at log on with
# -IntervalSeconds 300 so the loop runs every 5 minutes indefinitely.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1 -IntervalSeconds 300 -OutputBase "D:\Path"

param(
    [int]   $IntervalSeconds = 300,   # 5 minutes between fetches (open-90 to close+90 window)
    [string] $OutputBase     = ""     # default: from config/config.json or built-in
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe   = "python"
if ($OutputBase -eq "") {
    $DefaultOutputBase = "D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report"
    $CfgPath = Join-Path $ProjectRoot "config\config.json"
    if (Test-Path $CfgPath) {
        try {
            $cfg = Get-Content $CfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($cfg.output_base) { $DefaultOutputBase = $cfg.output_base }
        } catch { }
    }
    $OutputBase = $DefaultOutputBase
}

Set-Location $ProjectRoot

Write-Host "Queue-Times loop: interval=$IntervalSeconds s, output=$OutputBase"
Write-Host "Stop with Ctrl+C."
& $PythonExe src/get_wait_times_from_queue_times.py --output-base $OutputBase --interval $IntervalSeconds
