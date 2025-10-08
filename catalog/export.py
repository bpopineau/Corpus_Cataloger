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
    
    # Validate database path exists
    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    con = duckdb.connect(database=":memory:")
    # Use parameterized query to prevent SQL injection
    con.execute("ATTACH DATABASE ? AS cat (TYPE SQLITE);", (str(db_path),))
    con.execute("COPY (SELECT * FROM cat.files) TO ? (FORMAT PARQUET, OVERWRITE TRUE);", (str(out / "files.parquet"),))
    con.execute("COPY (SELECT * FROM cat.scans) TO ? (FORMAT PARQUET, OVERWRITE TRUE);", (str(out / "scans.parquet"),))
    con.close()
    print(f"[OK] Parquet written to {out}")

if __name__ == '__main__':
    main()
