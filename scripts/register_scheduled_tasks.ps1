# Register scheduled tasks for Theme Park Wait Time ETL
# Run daily at 5:00 AM and 7:00 AM Eastern (local time).
# Lock file prevents 7 AM run from conflicting if 5 AM is still running.

$ErrorActionPreference = "Stop"
$ProjectRoot = "d:\GitHub\hazeydata\theme-park-crowd-report"
$PythonExe   = "C:\Python314\python.exe"
$Script      = "src/get_tp_wait_time_data_from_s3.py"

$Action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $Script `
    -WorkingDirectory $ProjectRoot

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries

# 5:00 AM daily
$Trigger5 = New-ScheduledTaskTrigger -Daily -At "5:00AM"
Register-ScheduledTask `
    -TaskName "ThemeParkWaitTimeETL_5am" `
    -Action $Action `
    -Trigger $Trigger5 `
    -Settings $Settings `
    -Description "Theme Park Wait Time ETL - daily 5 AM Eastern. Incremental run; processes new/changed S3 files." `
    -Force
Write-Host "Registered: ThemeParkWaitTimeETL_5am (Daily 5:00 AM)"

# 7:00 AM daily (backup if 5 AM didn't run or S3 updates late)
$Trigger7 = New-ScheduledTaskTrigger -Daily -At "7:00AM"
Register-ScheduledTask `
    -TaskName "ThemeParkWaitTimeETL_7am" `
    -Action $Action `
    -Trigger $Trigger7 `
    -Settings $Settings `
    -Description "Theme Park Wait Time ETL - daily 7 AM Eastern. Backup run; lock prevents conflict with 5 AM." `
    -Force
Write-Host "Registered: ThemeParkWaitTimeETL_7am (Daily 7:00 AM)"

# 6:00 AM daily – dimension fetches (entity, park hours, events, metatable) + build dimdategroupid, dimseason
$DimScript = Join-Path $ProjectRoot "scripts\run_dimension_fetches.ps1"
$DimAction = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -NoProfile -File `"$DimScript`"" `
    -WorkingDirectory $ProjectRoot
$Trigger6 = New-ScheduledTaskTrigger -Daily -At "6:00AM"
Register-ScheduledTask `
    -TaskName "ThemeParkDimensionFetch_6am" `
    -Action $DimAction `
    -Trigger $Trigger6 `
    -Settings $Settings `
    -Description "Theme Park dimension tables - daily 6 AM Eastern. Fetches entity, park hours, events, metatable from S3; builds dimdategroupid, dimseason." `
    -Force
Write-Host "Registered: ThemeParkDimensionFetch_6am (Daily 6:00 AM)"

# Queue-Times.com scraper: run every 5 minutes via a loop (started at log on). Uses dimparkhours
# to only call the API when a park is in-window (open-90 to close+90 in park TZ).
$QtScript = Join-Path $ProjectRoot "scripts\run_queue_times_loop.ps1"
$QtAction = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -NoProfile -File `"$QtScript`" -IntervalSeconds 300" `
    -WorkingDirectory $ProjectRoot
$QtTrigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask `
    -TaskName "ThemeParkQueueTimes_5min" `
    -Action $QtAction `
    -Trigger $QtTrigger `
    -Settings $Settings `
    -Description "Queue-Times.com scraper - loop every 5 min when parks in-window (open-90 to close+90). Start at log on; stop with Ctrl+C or task kill." `
    -Force
Write-Host "Registered: ThemeParkQueueTimes_5min (At log on, interval 300s)"

Write-Host ""
Write-Host "Done. Tasks use local time; set system time zone to Eastern for 5/6/7 AM ET."
Write-Host "View in Task Scheduler: taskschd.msc -> Task Scheduler Library"
