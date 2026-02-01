@echo off
REM Crypto Data Platform - Quick Start
REM Run this from the project root directory

echo Starting Crypto Data Platform Pipeline...
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0scripts\start-pipeline.ps1" %*

if errorlevel 1 (
    echo.
    echo Pipeline startup failed. Check the errors above.
    pause
)
