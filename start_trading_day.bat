@echo off
rem NiftyTradingAI - live PAPER trading day: pre-flight checks, dashboard, then the engine (until 15:35 IST).
rem Double-click before 09:15 IST. Paper only: no orders are ever placed.
rem   start_trading_day.bat                 normal start
rem   start_trading_day.bat recover NAME    restart after a crash (journal is replayed and verified)
rem   start_trading_day.bat check           pre-flight checks only (e.g. the evening before)
cd /d "%~dp0"
title NiftyTradingAI - paper engine

if /i "%~1"=="check" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\start_market_day.ps1" -PreflightOnly
) else if /i "%~1"=="recover" (
    if "%~2"=="" (
        echo Usage: start_trading_day.bat recover YOUR_NAME
        pause
        exit /b 2
    )
    powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\start_market_day.ps1" -Recover -Actor "%~2"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\start_market_day.ps1"
)

echo.
echo Finished with exit code %ERRORLEVEL% (0 = OK). If started, the dashboard keeps running in its own window.
pause
