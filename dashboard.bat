@echo off
rem NiftyTradingAI dashboard only (read-only view of the databases) at http://127.0.0.1:8501
cd /d "%~dp0"
title NiftyTradingAI - dashboard
start "" http://127.0.0.1:8501
".venv\Scripts\python.exe" -m streamlit run app\dashboard\ui.py --server.headless true --server.address 127.0.0.1 --server.port 8501
pause
