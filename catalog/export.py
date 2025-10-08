from __future__ import annotations
import argparse
from pathlib import Path
import duckdb

def main():
    ap = argparse.ArgumentParser(description="Export SQLite tables to Parquet via DuckDB")
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    con.execute(f"ATTACH DATABASE '{args.db}' AS cat (TYPE SQLITE);")
    con.execute("COPY (SELECT * FROM cat.files) TO ? (FORMAT PARQUET, OVERWRITE TRUE);", (str(out / "files.parquet"),))
    con.execute("COPY (SELECT * FROM cat.scans) TO ? (FORMAT PARQUET, OVERWRITE TRUE);", (str(out / "scans.parquet"),))
    con.close()
    print(f"[OK] Parquet written to {out}")

if __name__ == '__main__':
    main()
