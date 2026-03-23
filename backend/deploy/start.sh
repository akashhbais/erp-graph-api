#!/usr/bin/env bash
set -e

DB_PATH="${APP_DB_PATH:-/tmp/app.duckdb}"
PORT_VALUE="${PORT:-8000}"

mkdir -p "$(dirname "$DB_PATH")"

if [ ! -f "$DB_PATH" ]; then
  echo "[start] DB not found at $DB_PATH. Running ingestion..."
  if [ -d "sap-order-to-cash-dataset/sap-o2c-data" ]; then
    python scripts/ingest_dataset.py --raw-dir sap-order-to-cash-dataset/sap-o2c-data --db-path "$DB_PATH"
  fi
fi

exec uvicorn backend.app.main:app --host 0.0.0.0 --port "$PORT_VALUE"