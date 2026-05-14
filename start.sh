#!/usr/bin/env bash
# Start both the FastAPI backend and the SvelteKit frontend dev server.

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataFlow Agents — starting dev environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Install frontend deps if node_modules is missing
if [ ! -d "$ROOT/frontend/node_modules" ]; then
  echo "▸ Installing frontend dependencies…"
  cd "$ROOT/frontend" && npm install
  cd "$ROOT"
fi

# Start backend in background
echo "▸ Starting FastAPI backend on http://localhost:8000"
cd "$ROOT"
"$ROOT/.venv/bin/uvicorn" api.main:app --reload --port 8000 &
BACKEND_PID=$!

# Start frontend
echo "▸ Starting SvelteKit frontend on http://localhost:5173"
cd "$ROOT/frontend" && npm run dev &
FRONTEND_PID=$!

echo ""
echo "  Backend  → http://localhost:8000"
echo "  Frontend → http://localhost:5173"
echo ""
echo "  Press Ctrl+C to stop both servers."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Wait and clean up on exit
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait
