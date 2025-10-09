from __future__ import annotations
import sqlite3
from pathlib import Path

DDL = r"""
CREATE TABLE IF NOT EXISTS scans (
  scan_run_id INTEGER PRIMARY KEY,
  started_at TEXT NOT NULL,
  root_path TEXT NOT NULL,
  host TEXT NOT NULL,
  user TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
  file_id INTEGER PRIMARY KEY,
  scan_run_id INTEGER NOT NULL REFERENCES scans(scan_run_id),
  path_abs TEXT NOT NULL,
  dir TEXT NOT NULL,
  name TEXT NOT NULL,
  ext TEXT,
  size_bytes INTEGER NOT NULL,
  mtime_utc TEXT NOT NULL,
  ctime_utc TEXT NOT NULL,
  owner TEXT,
  flags TEXT,
  mime_hint TEXT,
  quick_hash TEXT,
  sha256 TEXT,
  is_pdf_born_digital INTEGER,
  state TEXT NOT NULL,
  error_code TEXT,
  error_msg TEXT,
  last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_quick ON files(size_bytes, quick_hash);
CREATE INDEX IF NOT EXISTS idx_files_quick_hash ON files(quick_hash);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_size_ext ON files(size_bytes, ext);
CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path_abs);
CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime_utc);
CREATE INDEX IF NOT EXISTS idx_files_ctime ON files(ctime_utc);
"""

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    return con

def migrate(con: sqlite3.Connection) -> None:
  con.executescript(DDL)
  # Schema upgrades: add columns if missing
  cur = con.cursor()
  cur.execute("PRAGMA table_info(files)")
  cols = {row[1] for row in cur.fetchall()}
  upgrades = []
  if 'h1' not in cols:
    upgrades.append("ALTER TABLE files ADD COLUMN h1 TEXT")
  if 'h2' not in cols:
    upgrades.append("ALTER TABLE files ADD COLUMN h2 TEXT")
  if 'blake3' not in cols:
    upgrades.append("ALTER TABLE files ADD COLUMN blake3 TEXT")
  for sql in upgrades:
    cur.execute(sql)
  # Add indexes for new columns (create if not exists)
  cur.executescript(
    """
    CREATE INDEX IF NOT EXISTS idx_files_h1_size ON files(size_bytes, h1);
    CREATE INDEX IF NOT EXISTS idx_files_h1_h2_size ON files(size_bytes, h1, h2);
    CREATE INDEX IF NOT EXISTS idx_files_blake3 ON files(blake3);
    """
  )
  con.commit()
