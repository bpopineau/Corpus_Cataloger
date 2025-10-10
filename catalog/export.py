from __future__ import annotations
import argparse
from pathlib import Path
from typing import Iterable, Optional

import duckdb


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Export SQLite tables to Parquet via DuckDB")
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(list(argv) if argv is not None else None)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    
    # Validate database path exists
    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    con = duckdb.connect(database=":memory:")
    # DuckDB doesn't support parameter placeholders for ATTACH/COPY targets.
    # Safely quote paths by doubling single quotes.
    db_quoted = str(db_path).replace("'", "''")
    files_out_quoted = str(out / "files.parquet").replace("'", "''")
    scans_out_quoted = str(out / "scans.parquet").replace("'", "''")

    con.execute(f"ATTACH DATABASE '{db_quoted}' AS cat (TYPE SQLITE);")
    con.execute(f"COPY (SELECT * FROM cat.files) TO '{files_out_quoted}' (FORMAT PARQUET, OVERWRITE TRUE);")
    con.execute(f"COPY (SELECT * FROM cat.scans) TO '{scans_out_quoted}' (FORMAT PARQUET, OVERWRITE TRUE);")
    con.close()
    print(f"[OK] Parquet written to {out}")

if __name__ == '__main__':
    main()
