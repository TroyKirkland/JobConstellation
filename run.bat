@echo off
REM Start Job Constellation locally on http://localhost:8765
cd /d "%~dp0"
if not exist .venv (
  python -m venv .venv
  .venv\Scripts\pip install -q -r requirements.txt
)
start "" http://localhost:8765
.venv\Scripts\uvicorn server:app --port 8765
