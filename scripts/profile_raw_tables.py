from __future__ import annotations

from pathlib import Path
import duckdb

DB_PATH = Path("data/duckdb/app.duckdb")


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    try:
        tables = [
            r[0]
            for r in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_name LIKE 'raw_%'
                ORDER BY table_name
                """
            ).fetchall()
        ]

        if not tables:
            print("No raw_ tables found.")
            return

        for table in tables:
            print(f"===== {table} =====")

            cols = [
                r[1]
                for r in con.execute(f"PRAGMA table_info({qident(table)})").fetchall()
            ]
            print("Columns:")
            for c in cols:
                print(c)

            print("\nSample rows:")
            rows_df = con.execute(f"SELECT * FROM {qident(table)} LIMIT 3").df()
            if rows_df.empty:
                print("(no rows)")
            else:
                for row in rows_df.to_dict(orient="records"):
                    print(row)

            print()
    finally:
        con.close()


if __name__ == "__main__":
    main()