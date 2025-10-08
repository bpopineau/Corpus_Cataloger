# catalog/scan.py
from __future__ import annotations
import argparse, os, socket, getpass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import load_config, CatalogConfig
from .db import connect, migrate
from .util import quick_hash, sha256_file

def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def should_skip_path(p: Path, excludes: List[str]) -> bool:
    s = str(p)
    for pat in excludes:
        if pat and pat in s:
            return True
    return False

ProgressCallback = Callable[[str, int, int, str], None]
LogCallback = Callable[[str], None]


def _error_record(path: Path, code: str, message: str) -> Dict:
    now = datetime.utcnow().isoformat()+"Z"
    return dict(
        path_abs=str(path),
        dir=str(path.parent),
        name=path.name,
        ext=path.suffix.lower(),
        size_bytes=0,
        mtime_utc=now,
        ctime_utc=now,
        owner=None,
        flags=None,
        mime_hint=None,
        quick_hash=None,
        sha256=None,
        is_pdf_born_digital=None,
        state="error",
        error_code=code,
        error_msg=message,
        last_seen_at=now,
    )


def scan_root(root: str, cfg: CatalogConfig, progress_cb: Optional[ProgressCallback] = None, log_cb: Optional[LogCallback] = None) -> None:
    def emit_progress(stage: str, current: int, total: int, message: str) -> None:
        if not progress_cb:
            return
        try:
            progress_cb(stage, current, total, message)
        except Exception:
            pass

    def emit_log(message: str) -> None:
        print(message)
        if not log_cb:
            return
        try:
            log_cb(message)
        except Exception:
            pass

    emit_progress("start", 0, 0, "Preparing scan...")
    emit_log(
        "[RUN] Starting scan: root=%s, max_workers=%s, chunk_bytes=%s"
        % (root, cfg.scanner.max_workers, cfg.scanner.io_chunk_bytes)
    )
    db_path = Path(cfg.db.path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = connect(db_path)
    migrate(con)
    cur = con.cursor()

    cur.execute(
        "INSERT INTO scans(started_at, root_path, host, user) VALUES (?,?,?,?)",
        (datetime.utcnow().isoformat()+"Z", root, socket.gethostname(), getpass.getuser()),
    )
    scan_run_id = cur.lastrowid
    con.commit()

    include = set([e.lower() for e in cfg.include_ext]) if cfg.include_ext else None
    excludes = cfg.exclude_paths or []

    files_to_process: List[Path] = []
    root_path = Path(root)
    if not root_path.exists():
        emit_log(f"[WARN] Root does not exist: {root}")
        emit_progress("error", 0, 0, "Root path missing")
        return

    enumerated = 0
    dir_count = 0
    emit_log("[INFO] Enumerating filesystem...")
    emit_progress("enumerating", 0, 0, "Walking directories...")
    for dirpath, dirnames, filenames in os.walk(root):
        dpath = Path(dirpath)
        if should_skip_path(dpath, excludes):
            continue
        dir_count += 1
        for name in filenames:
            p = dpath / name
            try:
                if include and p.suffix.lower() not in include:
                    continue
                if should_skip_path(p, excludes):
                    continue
                files_to_process.append(p)
                enumerated += 1
                if enumerated % 250 == 0:
                    emit_progress(
                        "enumerating",
                        enumerated,
                        0,
                        f"Scanned {dir_count} folders, queued {enumerated} files",
                    )
            except Exception as e:
                emit_log(f"[WARN] Failed to consider {p}: {e}")
                continue

    total = len(files_to_process)
    emit_progress(
        "enumerating",
        enumerated,
        total,
        f"Indexed {total} files across {dir_count} folders",
    )
    emit_log(f"[INFO] {root}: {total} candidate files across {dir_count} folders")

    def process(path: Path) -> Dict:
        try:
            st = path.stat()
            size = st.st_size
            mtime = utc(st.st_mtime)
            ctime = utc(st.st_ctime)
            qh = quick_hash(path, cfg.scanner.io_chunk_bytes)
            ext = path.suffix.lower()
            return dict(
                path_abs=str(path),
                dir=str(path.parent),
                name=path.name,
                ext=ext,
                size_bytes=size,
                mtime_utc=mtime,
                ctime_utc=ctime,
                owner=None,
                flags=None,
                mime_hint=None,
                quick_hash=qh,
                sha256=None,
                is_pdf_born_digital=None,
                state="quick_hashed",
                error_code=None,
                error_msg=None,
                last_seen_at=datetime.utcnow().isoformat()+"Z",
            )
        except Exception as e:
            return _error_record(path, "process", str(e))

    batch_rows: List[Dict] = []
    batch_size = 1000
    inserted_total = 0
    if total:
        emit_progress("processing", 0, total, f"Hashing metadata for {total} files")
        emit_log(f"[INFO] Processing {total} files with up to {cfg.scanner.max_workers} workers")
    else:
        emit_progress("processing", 0, 0, "No files to process")
        emit_log("[INFO] Nothing to process; skipping hashing stage")

    emit_progress("database", inserted_total, total, "Waiting for first batch...")

    def flush_batch() -> None:
        nonlocal inserted_total
        if not batch_rows:
            return
        batch_size_now = len(batch_rows)
        insert_batch(batch_rows)
        inserted_total += batch_size_now
        batch_rows.clear()
        if total > 0:
            emit_progress("database", inserted_total, total, f"Inserted {inserted_total}/{total} records")
            emit_log(f"[DB] inserted {inserted_total}/{total}")
        else:
            emit_progress("database", inserted_total, inserted_total, f"Inserted {inserted_total} records")
            emit_log(f"[DB] inserted {inserted_total} records")

    with ThreadPoolExecutor(max_workers=cfg.scanner.max_workers) as ex:
        fut_map = {ex.submit(process, p): p for p in files_to_process}
        for i, fut in enumerate(as_completed(fut_map), 1):
            try:
                row = fut.result()
            except Exception as e:
                path = fut_map.get(fut)
                if path is not None:
                    row = _error_record(path, "process", str(e))
                else:
                    now = datetime.utcnow().isoformat()+"Z"
                    row = dict(
                        path_abs="",
                        dir="",
                        name="",
                        ext="",
                        size_bytes=0,
                        mtime_utc=now,
                        ctime_utc=now,
                        owner=None,
                        flags=None,
                        mime_hint=None,
                        quick_hash=None,
                        sha256=None,
                        is_pdf_born_digital=None,
                        state="error",
                        error_code="process",
                        error_msg=str(e),
                        last_seen_at=now,
                    )
            batch_rows.append(row)
            if len(batch_rows) >= batch_size:
                flush_batch()
            if total:
                emit_progress("processing", i, total, f"Processed {i} of {total} files")
            if total and (i % 500 == 0 or i == total):
                emit_log(f"[PROCESS] {i}/{total} files processed")

    flush_batch()

    def insert_batch(rows: List[Dict]):
        if not rows:
            return
        cur.executemany(
            """INSERT INTO files
            (scan_run_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc,
             owner, flags, mime_hint, quick_hash, sha256, is_pdf_born_digital, state, error_code, error_msg, last_seen_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (scan_run_id, r.get("path_abs"), r.get("dir"), r.get("name"), r.get("ext"), r.get("size_bytes"),
                 r.get("mtime_utc"), r.get("ctime_utc"), r.get("owner"), r.get("flags"), r.get("mime_hint"),
                 r.get("quick_hash"), r.get("sha256"), r.get("is_pdf_born_digital"), r.get("state"),
                 r.get("error_code"), r.get("error_msg"), r.get("last_seen_at"))
                for r in rows
            ]
        )
        con.commit()

    total_records = inserted_total

    # Compute sha256 for groups that look like duplicates (same size + quick_hash)
    cur.execute(
        "SELECT size_bytes, quick_hash, COUNT(*) AS n FROM files "
        "WHERE scan_run_id = ? GROUP BY size_bytes, quick_hash HAVING n > 1",
        (scan_run_id,),
    )
    groups = cur.fetchall()
    emit_log(f"[INFO] duplicate candidate groups this run: {len(groups)}")

    duplicate_jobs: List[Tuple[int, str]] = []
    for size, qh, n in groups:
        cur.execute(
            "SELECT file_id, path_abs FROM files WHERE scan_run_id = ? AND size_bytes = ? AND quick_hash = ? AND sha256 IS NULL",
            (scan_run_id, size, qh),
        )
        rows = cur.fetchall()
        duplicate_jobs.extend(rows)

    total_sha = len(duplicate_jobs)
    processed_sha = 0
    if total_sha:
        emit_progress("dedupe", 0, total_sha, f"Computing sha256 for {total_sha} duplicate candidates")
        emit_log(f"[INFO] Computing sha256 for {total_sha} candidate duplicates")
        for file_id, path_abs in duplicate_jobs:
            try:
                digest = sha256_file(Path(path_abs))
                cur.execute("UPDATE files SET sha256=?, state='done' WHERE file_id=?", (digest, file_id))
            except Exception as e:
                cur.execute(
                    "UPDATE files SET state='error', error_code='sha256', error_msg=? WHERE file_id=?",
                    (str(e), file_id),
                )
            processed_sha += 1
            if processed_sha % 20 == 0 or processed_sha == total_sha:
                emit_progress(
                    "dedupe",
                    processed_sha,
                    total_sha,
                    f"SHA complete for {processed_sha}/{total_sha} duplicates",
                )
        con.commit()
    else:
        emit_progress("dedupe", 0, 0, "No duplicate groups detected")
        emit_log("[INFO] No duplicate candidates detected this run")

    # Mark remaining as done
    emit_progress("finalize", 0, 0, "Finalizing states...")
    cur.execute("UPDATE files SET state='done' WHERE scan_run_id=? AND state='quick_hashed'", (scan_run_id,))
    con.commit()
    con.close()
    emit_progress("done", total_records, total if total else total_records, "Scan complete")
    emit_log("[DONE] scan complete.")

def main():
    ap = argparse.ArgumentParser(description="Corpus Cataloger - Scanner")
    ap.add_argument("--config", required=True)
    ap.add_argument("--max-workers", type=int, default=None)
    ap.add_argument("--root", action="append")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    if args.max_workers:
        cfg.scanner.max_workers = args.max_workers
    roots = cfg.roots[:]
    if args.root:
        roots.extend(args.root)

    for r in roots:
        print(f"[RUN] scanning root: {r}")
        scan_root(r, cfg)

if __name__ == "__main__":
    main()
