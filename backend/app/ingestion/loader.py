from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, List
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

def _safe_name(name: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in name.lower())
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")

def _read_file(f: Path) -> pd.DataFrame:
    ext = f.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(f, dtype="string", low_memory=False)
    if ext == ".jsonl":
        return pd.read_json(f, lines=True, dtype="string")
    raise ValueError(f"Unsupported file type: {f}")

def load_raw_csvs(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> Dict[str, pd.DataFrame]:
    raw_dir = Path(raw_dir)
    files = sorted([p for p in raw_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".csv", ".jsonl")])

    if not files:
        logger.warning("No CSV/JSONL files found in %s", raw_dir)
        return {}

    grouped: Dict[str, List[pd.DataFrame]] = {}

    # Group by parent folder (e.g. billing_document_headers, products, outbound_delivery_items)
    for f in files:
        key = _safe_name(f.parent.name)
        try:
            df = _read_file(f)
        except Exception as ex:
            logger.warning("Skipping unreadable file %s: %s", f, ex)
            continue

        grouped.setdefault(key, []).append(df)

    frames: Dict[str, pd.DataFrame] = {}
    for key, parts in grouped.items():
        if not parts:
            continue
        df = pd.concat(parts, ignore_index=True)
        frames[key] = df

        con.register("_raw_df", df)
        con.execute(f"CREATE OR REPLACE TABLE raw_{key} AS SELECT * FROM _raw_df")
        con.unregister("_raw_df")

        logger.info("Created raw_%s (%s rows, %s columns)", key, len(df), len(df.columns))

    logger.info("Loaded %s logical raw tables from %s files", len(frames), len(files))
    return frames
