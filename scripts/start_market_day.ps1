# Start a live PAPER trading day: pre-flight -> dashboard (own window) -> engine (this window, until 15:35 IST).
#
#   powershell -ExecutionPolicy Bypass -File scripts\start_market_day.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\start_market_day.ps1 -PreflightOnly      # evening check
#   powershell -ExecutionPolicy Bypass -File scripts\start_market_day.ps1 -Recover -Actor anoop   # after a crash
#
# Paper only: no order API is ever called. Start between 08:45 and 09:10 IST. The engine itself records the
# market feed, so do NOT also run record_session.py (single-writer rule).
param(
    [switch]$Recover,
    [string]$Actor = "",
    [switch]$NoDashboard,
    [switch]$SkipPreflight,
    [switch]$PreflightOnly
)

$ErrorActionPreference = "Continue"   # python logs to stderr; "Stop" would abort on the first line
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$py = Join-Path $root ".venv\Scripts\python.exe"
$env:PYTHONIOENCODING = "utf-8"
# the engine prints UTF-8 (rupee sign etc.); without this PowerShell decodes it with the OEM code page (mojibake)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$logDir = Join-Path $root "data\logs"
New-Item -ItemType Directory -Force $logDir | Out-Null

# the venv must start at all (its base interpreter lives in .python\, see README Setup)
& $py -c "import sys" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Python in .venv does not start (exit $LASTEXITCODE). Check .venv\pyvenv.cfg 'home' points to an existing python folder." -ForegroundColor Red
    exit 2
}

if (-not $SkipPreflight) {
    & $py scripts\preflight.py --fix
    $code = $LASTEXITCODE
    if ($code -ne 0 -and $code -ne 1) {        # 2 = a check failed; anything else = pre-flight itself crashed
        Write-Host "`nPre-flight FAILED (exit $code) - fix the [FAIL] lines above, then re-run." -ForegroundColor Red
        exit 2
    }
    if ($code -eq 1) { Write-Host "`nPre-flight has warnings (see above)." -ForegroundColor Yellow }
}
if ($PreflightOnly) { exit 0 }

if (-not $NoDashboard) {
    $dash = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue
    if (-not $dash) {
        Start-Process -FilePath $py -ArgumentList "-m", "streamlit", "run", "app\dashboard\ui.py",
            "--server.headless", "true", "--server.address", "127.0.0.1", "--server.port", "8501" `
            -WorkingDirectory $root -WindowStyle Minimized
        Write-Host "dashboard: http://127.0.0.1:8501"
        Start-Sleep -Seconds 4
        Start-Process "http://127.0.0.1:8501"
    } else {
        Write-Host "dashboard already running: http://127.0.0.1:8501"
    }
}

$engineArgs = @("scripts\run_paper.py", "live")
if ($Recover) {
    if (-not $Actor) { Write-Host "-Recover needs -Actor NAME" -ForegroundColor Red; exit 2 }
    $engineArgs += @("--recover", "--actor", $Actor)
}
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$log = Join-Path $logDir "engine_console_$stamp.log"
Write-Host "engine starting (console copy: $log). Ctrl+C stops it; restart with -Recover -Actor NAME."
# loguru writes to stderr; with "Stop", PowerShell 5.1 would turn the first log line into a terminating error
$ErrorActionPreference = "Continue"
& $py @engineArgs 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $log
exit $LASTEXITCODE
