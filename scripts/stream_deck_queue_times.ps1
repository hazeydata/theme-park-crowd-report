# Stream Deck launcher for the Queue-Times.com scraper loop.
# Uses the script's location to find the project root, so it works no matter
# where Stream Deck sets "Start in". Run via start_queue_times_stream_deck.bat
# (or PowerShell with -NoExit) so the window stays open for output and Ctrl+C.
#
# Usage from Stream Deck: point the button at scripts\start_queue_times_stream_deck.bat
# Or: powershell -NoExit -ExecutionPolicy Bypass -File "path\to\scripts\stream_deck_queue_times.ps1"

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# Use full path to Python when available so it works when launched from Stream Deck
# (Stream Deck often doesn't inherit a PATH with python). Edit this line if your
# Python lives elsewhere.
$PythonExe = "python"
if (Test-Path "C:\Users\fred\AppData\Local\Programs\Python\Python311\python.exe") {
    $PythonExe = "C:\Users\fred\AppData\Local\Programs\Python\Python311\python.exe"
}

$DefaultOutputBase = "D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report"
$CfgPath = Join-Path $ProjectRoot "config\config.json"
if (Test-Path $CfgPath) {
    try {
        $cfg = Get-Content $CfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($cfg.output_base) { $DefaultOutputBase = $cfg.output_base }
    } catch { }
}
$OutputBase = $DefaultOutputBase
$IntervalSeconds = 300

Write-Host "Queue-Times loop (Stream Deck): interval=$IntervalSeconds s, output=$OutputBase"
Write-Host "Stop with Ctrl+C."
& $PythonExe src/get_wait_times_from_queue_times.py --output-base $OutputBase --interval $IntervalSeconds
