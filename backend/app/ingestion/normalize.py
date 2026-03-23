from __future__ import annotations
import pandas as pd

def _to_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()

def normalize_id(s: pd.Series) -> pd.Series:
    return _to_str(s).replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA})

def normalize_currency(s: pd.Series) -> pd.Series:
    return _to_str(s).str.upper().str.replace(r"[^A-Z]", "", regex=True).str[:3].replace({"": pd.NA})

def normalize_decimal(s: pd.Series, scale: int = 2) -> pd.Series:
    x = _to_str(s).str.replace(",", "", regex=False)
    x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    x = x.str.replace(r"[^0-9\.\-]", "", regex=True)
    return pd.to_numeric(x, errors="coerce").round(scale)

def normalize_int(s: pd.Series) -> pd.Series:
    x = _to_str(s).str.extract(r"(-?\d+)")[0]
    return pd.to_numeric(x, errors="coerce").astype("Int64")

def normalize_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True).dt.date

def normalize_timestamp(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)

def clean_text(s: pd.Series) -> pd.Series:
    return _to_str(s).replace({"": pd.NA})
