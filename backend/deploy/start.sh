#!/usr/bin/env bash
set -e

DB_PATH="${APP_DB_PATH:-/var/data/app.duckdb}"
PORT_VALUE="${PORT:-8000}"

mkdir -p "$(dirname "$DB_PATH")"

if [ ! -f "$DB_PATH" ]; then
  echo "[start] DB not found at $DB_PATH. Running ingestion..."
  if [ -d "sap-order-to-cash-dataset/sap-o2c-data" ]; then
    python scripts/ingest_dataset.py --raw-dir sap-order-to-cash-dataset/sap-o2c-data --db-path "$DB_PATH"
  elif [ -d "sap-order-to-cash-dataset" ]; then
    python scripts/ingest_dataset.py --raw-dir sap-order-to-cash-dataset --db-path "$DB_PATH"
  else
    echo "[start] Dataset folder not found. Starting without ingestion."
  fi
fi

exec uvicorn backend.app.main:app --host 0.0.0.0 --port "$PORT_VALUE"