#!/usr/bin/env bash
set -e

DB_PATH="${APP_DB_PATH:-/var/data/app.duckdb}"
PORT_VALUE="${PORT:-8000}"

mkdir -p "$(dirname "$DB_PATH")"

# Optional: initialize DB if missing
if [ ! -f "$DB_PATH" ]; then
  echo "[start] DB not found at $DB_PATH"
  # python scripts/ingest_dataset.py --raw-dir sap-order-to-cash-dataset --db-path "$DB_PATH"
fi

exec uvicorn backend.app.main:app --host 0.0.0.0 --port "$PORT_VALUE"