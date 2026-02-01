@echo off
REM Crypto Data Platform - Stop All Services

echo Stopping Crypto Data Platform Pipeline...
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0scripts\start-pipeline.ps1" -Stop

echo.
echo Done.
pause
