@echo off
REM Launches the Queue-Times scraper loop in a PowerShell window that stays open.
REM Stop the loop with Ctrl+C; close the window when done.
REM
REM Stream Deck: use "Open" with this .bat, OR (if no window appears) set the
REM button to run cmd.exe with:  /k "D:\GitHub\hazeydata\theme-park-crowd-report\scripts\start_queue_times_stream_deck.bat"
cd /d "%~dp0"
start "Queue-Times Loop" powershell -NoExit -ExecutionPolicy Bypass -File "%~dp0stream_deck_queue_times.ps1"
