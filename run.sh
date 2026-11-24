#!/bin/sh
# Start Job Constellation locally on http://localhost:8765
cd "$(dirname "$0")"
if [ ! -d .venv ]; then
  python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt || exit 1
fi
PORT="${PORT:-8765}"
( sleep 2; command -v open >/dev/null && open "http://localhost:$PORT" || xdg-open "http://localhost:$PORT" 2>/dev/null ) &
exec .venv/bin/uvicorn server:app --port "$PORT"
