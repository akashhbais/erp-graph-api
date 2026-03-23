#!/usr/bin/env bash
set -e

echo "Starting ERP Graph API..."

DB_PATH="${APP_DB_PATH:-/tmp/app.duckdb}"
PORT_VALUE="${PORT:-10000}"

echo "Database path: $DB_PATH"
echo "Port: $PORT_VALUE"

# Ensure directory exists
mkdir -p "$(dirname "$DB_PATH")"

# If DB not present, build it from dataset
if [ ! -f "$DB_PATH" ]; then
  echo "[start] DB not found. Running ingestion..."
  
  if [ -d "sap-order-to-cash-dataset/sap-o2c-data" ]; then
    python scripts/ingest_dataset.py \
      --raw-dir sap-order-to-cash-dataset/sap-o2c-data \
      --db-path "$DB_PATH"
  else
    echo "[start] Dataset not found, using bundled DB if available"
    if [ -f "data/duckdb/app.duckdb" ]; then
      cp data/duckdb/app.duckdb "$DB_PATH"
    fi
  fi
fi

export APP_DB_PATH="$DB_PATH"

echo "Launching FastAPI server..."

exec uvicorn backend.app.main:app --host 0.0.0.0 --port "$PORT_VALUE"