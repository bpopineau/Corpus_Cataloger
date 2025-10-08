# Corpus Cataloger v0.1
**Map your entire company server, fast and safely.**  
Catalog file facts (path, size, timestamps, owner, extension), detect likely duplicates, and tag born-digital PDFs—without modifying a single file.

> **Why this exists:** You have hundreds of thousands (maybe millions) of files. Before any intelligent processing, you need a reliable, resumable inventory you can trust. This tool is that backbone.

---

## Table of Contents
- [Features](#features)
- [What It Does (and What It Doesn’t)](#what-it-does-and-what-it-doesnt)
- [Architecture](#architecture)
- [Data Model (SQLite)](#data-model-sqlite)
- [Install](#install)
- [Configure](#configure)
- [Run](#run)
- [PySide6 GUI](#pyside6-gui)
- [Progress Tracking](#progress-tracking)
- [Stop, Resume, Recover](#stop-resume-recover)
- [Performance & Speed Strategy](#performance--speed-strategy)
- [Basic Analysis Recipes](#basic-analysis-recipes)
- [Operational Safety](#operational-safety)
- [Roadmap](#roadmap)
- [FAQ](#faq)
- [License](#license)

---

## Features
- **Read-only, zero-risk** scanning (no renames, deletes, or writes near your files).
- **SQLite** single-source-of-truth with indexes and constraints.
- **Two-tier duplicate detection:** fast `quick_hash` (xxhash64 over size+first/last 64KB) + verified `sha256`.
- **Born-digital PDF flag** (simple text check for first N pages).
- **Resumable by design:** directory cursors + per-file state → pause anytime, resume later.
- **PySide6 GUI** (simple & clean): live counters, progress bars, throughput, ETA, Pause/Resume/Stop.
- **Optional Parquet export** for rapid ad-hoc analysis (DuckDB/PowerBI/Excel).
- **Extensible**: scanners and analyzers are pluggable; safe to scale to multiple roots.

---

## What It Does (and What It Doesn’t)
**Does**
- Walks one or more root folders you choose (UNC shares and mapped drives).
- Records: path, size, timestamps, owner, flags, extension, MIME hint.
- Computes `quick_hash` for all files; computes `sha256` only where duplicates are suspected.
- Stores errors (permission, long path, sharing violation) per file.
- GUI or CLI operation; same database.

**Does NOT**
- OCR, LLM, text extraction, content indexing (future modules).
- Change, move, or delete any file.
- Follow symlinks/junctions (prevents loop storms).

---

## Architecture
```
[Roots] --> [Scanner] --(file facts)--> [SQLite DB]
                       \--(quick_hash)--> [dup groups] --(sha256 verify)--> [exact dupes]
                                   \--> [PDF text probe]--> [born_digital flag]
GUI <-------------- progress, ETA, logs --------------> DB
```

**Key design choices**
- **Idempotent:** files keyed by content and path; safe to re-run.
- **Incremental:** only new/changed files re-processed (size+mtime guard).
- **Polite to NAS:** bounded concurrency + backoff on sharing violations.

---

## Data Model (SQLite)

### Tables
```sql
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
  flags TEXT,           -- JSON: {read_only, hidden, system}
  mime_hint TEXT,
  quick_hash TEXT,      -- hex xxhash64 over (size + first/last 64KB)
  sha256 TEXT,          -- filled only when dup-group verified
  is_pdf_born_digital INTEGER, -- 1/0/NULL (not a PDF)
  state TEXT NOT NULL,  -- pending|quick_hashed|sha_pending|done|error
  error_code TEXT,
  error_msg TEXT,
  last_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_quick ON files(size_bytes, quick_hash);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
```

**State machine (per file)**
- `pending` → `quick_hashed` → (`sha_pending` → `done`) or directly `done` if unique
- `error` on failure, with `error_code`/`error_msg` captured
- Rescan bumps `last_seen_at` and updates facts/timestamps as needed.

---

## Install

> **Windows + PowerShell (preferred)**

```powershell
# 1) Working directory (local SSD recommended)
$Root = "C:\ESSCO-Catalog"
New-Item -ItemType Directory -Force -Path "$Root\data","$Root\logs","$Root\config" | Out-Null

# 2) Python environment
py -3.12 -m venv "$Root\.venv"
. "$Root\.venv\Scripts\Activate.ps1"
pip install --upgrade pip

# 3) Required packages
pip install pyside6 pydantic xxhash python-magic-bin pypdf python-docx pyarrow duckdb rich tqdm

# 4) (Optional) SQLite CLI for quick local queries
# winget install SQLite.SQLiteCommandLine
```

---

## Configure
Create `config\catalog.yaml`:

```yaml
roots:
  - "\\ESSCO-SRV01\Projects"
  - "\\ESSCO-SRV01\Docs"
  - "S:\\Standards"

include_ext:
  - .pdf
  - .docx
  - .xlsx
  - .msg
  - .dwg
  - .png
  - .jpg
  - .tiff

exclude_paths:
  - "\\$RECYCLE.BIN\\"
  - "\\node_modules\\"
  - "\\\.git\\"
  - "\\Temp\\"
  - "\\~$"        # Office temp files

scanner:
  max_workers: 8          # tune per hardware
  io_chunk_bytes: 65536
  probe_pdf_pages: 3      # quick born-digital check
  compute_sha_for_top_dupe_groups_only: true

db:
  path: "C:\\ESSCO-Catalog\\data\\projects.db"
  journal_mode: "WAL"
  synchronous: "NORMAL"

export:
  parquet_dir: "C:\\ESSCO-Catalog\\data\\parquet"
  schedule: "manual"      # manual | daily | weekly
```

---

## Run

### CLI (fastest path)
```powershell
# First run (full scan over configured roots)
python -m catalog.scan --config "C:\ESSCO-Catalog\config\catalog.yaml"

# Resume (auto, based on file states)
python -m catalog.scan --config "C:\ESSCO-Catalog\config\catalog.yaml" --resume

# Export Parquet snapshots
python -m catalog.export --db "C:\ESSCO-Catalog\data\projects.db" --out "C:\ESSCO-Catalog\data\parquet"
```

**Common flags**
- `--roots \\server\share S:\OtherRoot` (override config roots)
- `--max-workers 12` (temporary override)
- `--since "2025-09-01"` (only changed since date)

### Schedule (Task Scheduler)
Create a nightly task that runs:
```powershell
. "C:\ESSCO-Catalog\.venv\Scripts\Activate.ps1"
python -m catalog.scan --config "C:\ESSCO-Catalog\config\catalog.yaml" --resume
python -m catalog.export --db "C:\ESSCO-Catalog\data\projects.db" --out "C:\ESSCO-Catalog\data\parquet"
```

---

## PySide6 GUI
A single, clean window that mirrors the CLI but adds live visibility and control.

**Layout**
- **Top bar:** Root selector (multi), Start, Pause, Resume, Stop.
- **Main panel (Progress):**
  - Overall progress bar (% files touched this run).
  - **Counters:** total files discovered, processed, done, errors, dup-groups pending/verified.
  - **Throughput & ETA:** files/min, MB/s, rolling 60-s average; ETA derived from remaining states.
  - **Queue meters:** pending → quick_hashed → sha_pending → done.
- **Right pane (Details):**
  - Live log tail (info/warn/error), filterable.
  - “Top duplicate groups” preview (auto-refresh every N seconds).
- **Bottom bar:** current worker count (with safe adjuster), NAS courtesy mode (on/off).

**Design principles**
- Native fonts (Segoe UI), subtle accent for primary actions only.
- No custom painting beyond spacing and hover/focus QSS.
- Keyboard accessible; all long work in threads (`QThreadPool`).

---

## Progress Tracking
- **Granular states** visible in the GUI (counts + list).
- **Persistent progress:** file states saved immediately—safe to kill the app without losing position.
- **Scan run record:** `scans` row per run; the GUI shows the active run and the last successful run.
- **ETA logic:** based on per-state throughput over the last N minutes (adaptive).

---

## Stop, Resume, Recover
- **Pause**: stops dequeuing new work; lets in-flight tasks finish; safe to Resume.
- **Stop**: attempts graceful cancel; incomplete reads are discarded; file state remains `pending` or last safe state.
- **Resume**:
  - Picks up files in `pending`, `sha_pending` (if verification was mid-flight), and any `quick_hashed` that haven’t been advanced.
  - Re-checks `(size_bytes, mtime)` before skipping; if changed, re-processes.
- **Crash/Power loss**: WAL journaling plus per-file state prevents corruption; on next start, an integrity sweep cleans partial records.

---

## Performance & Speed Strategy
Accuracy stays #1. We only do expensive work when evidence says it’s necessary.

1) **Two-tier hashing**
- Compute `quick_hash` for **every** file (O(128KB) I/O typical).
- Only compute `sha256` inside `(size, quick_hash)` groups with cardinality > 1.
- Skip `sha256` for unique groups → saves massive I/O.

2) **I/O & CPU parallelism**
- ThreadPool with bounded workers:
  - Light tasks (stat, quick hash) get more concurrency.
  - Heavy tasks (sha256 on big files) get fewer slots.
- Overlapped I/O on Windows via Python’s buffered reads; chunk size configurable.

3) **Avoid rework**
- Store `quick_hash` + `sha256` keyed by `(path, size, mtime, ctime)`.
- If none changed, **do not** re-hash.
- Deduplicate `sha256` reads across duplicate paths pointing to the same file metadata.

4) **Database efficiency**
- Batch inserts in transactions (1–5k rows per commit).
- `PRAGMA journal_mode=WAL`, `synchronous=NORMAL` for speed with safety.
- Indices created once; analyzer runs after large batches.

5) **NAS courtesy**
- Short backoff on `SharingViolation`.
- Optional per-share rate limit (MB/s).
- Skips known hot folders during business hours (configurable time windows).

6) **Born-digital probe**
- PDFs only; first N pages; if any text node is returned → flag true.
- No OCR attempted in v0.1.

7) **Hardware tuning (optional)**
- This tool can benefit from:
  - **NVMe SSD** for the DB and Parquet output.
  - **High-core CPU** (hashing parallelism).
  - **RAM** (file listings, batching).
- You can set `max_workers` to roughly `min(2 × physical cores, 16)` and adjust after a 10-minute warmup.
- If you want a tailored profile, provide:
  - CPU model & core count
  - System RAM
  - System drive (NVMe/SATA) and NAS link speed (1/2.5/10GbE)

---

## Basic Analysis Recipes

**Exact duplicates (by verified SHA-256)**
```sql
SELECT sha256, COUNT(*) AS n, SUM(size_bytes) AS bytes
FROM files WHERE sha256 IS NOT NULL
GROUP BY sha256 HAVING COUNT(*) > 1
ORDER BY bytes DESC, n DESC;
```

**Likely dupes needing verification**
```sql
SELECT size_bytes, quick_hash, COUNT(*) AS n
FROM files
GROUP BY size_bytes, quick_hash
HAVING COUNT(*) > 1
ORDER BY n DESC;
```

**Top 100 largest files**
```sql
SELECT path_abs, size_bytes
FROM files
ORDER BY size_bytes DESC
LIMIT 100;
```

**Extension distribution by space**
```sql
SELECT LOWER(COALESCE(ext,'')) AS ext, COUNT(*) AS n, SUM(size_bytes) AS bytes
FROM files GROUP BY ext ORDER BY bytes DESC;
```

**PDF born-digital coverage**
```sql
SELECT is_pdf_born_digital, COUNT(*) AS n
FROM files WHERE LOWER(ext)='.pdf'
GROUP BY is_pdf_born_digital;
```

**Error heatmap**
```sql
SELECT error_code, COUNT(*) AS n
FROM files WHERE error_code IS NOT NULL
GROUP BY error_code ORDER BY n DESC;
```

---

## Operational Safety
- **Read-only by design.** No file writes on network shares.
- **No symlink/junction follow.** Prevents infinite loops and cross-mount storms.
- **Path length safe.** Uses extended prefix (`\\?\`) for long paths.
- **Per-file error capture.** Permissions, transient locks—logged, not fatal.
- **Audit trail.** Every run stamped in `scans`; file `last_seen_at` updated on touch.

---

## Roadmap
- v0.2: Multi-host distributed scanning (coordinated by DB), richer MIME detection.
- v0.3: “Top Dupe Groups” GUI page with suggested keep/delete **report** (still read-only).
- v0.4: Text harvesting for **born-digital** PDFs/DOCX (sidecars + Parquet).
- v0.5: First doc-type plugin (RFI cover) using harvested text (rule-first).

---

## FAQ

**Q: Will this slow down the NAS?**  
A: Concurrency and read sizes are configurable. Start conservative (e.g., 4–6 workers). The GUI shows throughput—tune from there.

**Q: Can I stop it mid-scan?**  
A: Yes. Hit **Pause** to drain the queue or **Stop** to cancel in-flight work safely. Resume later; progress is persisted.

**Q: Does it hash every file fully?**  
A: No. Full `sha256` only runs inside suspected duplicate groups. Unique files only get the lightweight `quick_hash`.

**Q: How do I get quick summaries without SQL?**  
A: Use the Parquet export and open in DuckDB or Power BI. Recipes are provided above.

---

## License
Internal ESSCO tool. All rights reserved.
