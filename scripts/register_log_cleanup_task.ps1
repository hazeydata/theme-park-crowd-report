# Register log cleanup scheduled task
# Run this script as Administrator (right-click PowerShell -> Run as Administrator)

$ErrorActionPreference = "Stop"
$ProjectRoot = "d:\GitHub\hazeydata\theme-park-crowd-report"
$Python311Exe = "C:\Users\fred\AppData\Local\Programs\Python\Python311\python.exe"
$CleanupScript = Join-Path $ProjectRoot "scripts\cleanup_logs.py"

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator." -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator', then run this script again." -ForegroundColor Yellow
    exit 1
}

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries

$CleanupAction = New-ScheduledTaskAction `
    -Execute $Python311Exe `
    -Argument "`"$CleanupScript`" --days 30 --keep-recent 10" `
    -WorkingDirectory $ProjectRoot

$CleanupTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "7:00AM"

try {
    Register-ScheduledTask `
        -TaskName "ThemeParkLogCleanup_Sunday" `
        -Action $CleanupAction `
        -Trigger $CleanupTrigger `
        -Settings $Settings `
        -Description "Log Cleanup - weekly Sunday 7:00 AM Eastern. Deletes logs older than 30 days, keeps 10 most recent per log type." `
        -Force
    Write-Host "SUCCESS: Registered ThemeParkLogCleanup_Sunday (Weekly Sunday 7:00 AM)" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Cyan
    Write-Host "  Name: ThemeParkLogCleanup_Sunday"
    Write-Host "  Schedule: Weekly on Sunday at 7:00 AM"
    Write-Host "  Script: $CleanupScript"
    Write-Host "  Arguments: --days 30 --keep-recent 10"
    Write-Host ""
    Write-Host "View in Task Scheduler: taskschd.msc -> Task Scheduler Library -> ThemeParkLogCleanup_Sunday" -ForegroundColor Yellow
} catch {
    Write-Host "ERROR: Failed to register ThemeParkLogCleanup_Sunday: $_" -ForegroundColor Red
    exit 1
}
