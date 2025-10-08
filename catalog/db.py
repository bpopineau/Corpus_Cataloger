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
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
"""

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def migrate(con: sqlite3.Connection) -> None:
    con.executescript(DDL)
    con.commit()
