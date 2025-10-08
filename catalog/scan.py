# catalog/scan.py
from __future__ import annotations
import argparse, os, socket, getpass
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timezone
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import load_config, CatalogConfig
from .db import connect, migrate
from .util import quick_hash, sha256_file

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def should_skip_path(p: Path, excludes: List[str]) -> bool:
    s = str(p)
    for pat in excludes:
        if pat and pat in s:
            return True
    return False

def born_digital_pdf(path: Path, pages: int) -> Optional[int]:
    if PdfReader is None:
        return None
    try:
        r = PdfReader(str(path))
        n = min(len(r.pages), max(1, pages))
        for i in range(n):
            t = r.pages[i].extract_text() or ""
            if t.strip():
                return 1
        return 0
    except Exception:
        return None

def scan_root(root: str, cfg: CatalogConfig) -> None:
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
        print(f"[WARN] Root does not exist: {root}")
        return

    for dirpath, dirnames, filenames in os.walk(root):
        dpath = Path(dirpath)
        if should_skip_path(dpath, excludes):
            continue
        for name in filenames:
            p = dpath / name
            try:
                if include and p.suffix.lower() not in include:
                    continue
                if should_skip_path(p, excludes):
                    continue
                files_to_process.append(p)
            except Exception:
                continue

    total = len(files_to_process)
    print(f"[INFO] {root}: {total} candidate files")

    def process(path: Path) -> Dict:
        st = path.stat()
        size = st.st_size
        mtime = utc(st.st_mtime)
        ctime = utc(st.st_ctime)
        qh = quick_hash(path, cfg.scanner.io_chunk_bytes)
        ext = path.suffix.lower()
        is_pdf_bd = None
        if ext == ".pdf":
            is_pdf_bd = born_digital_pdf(path, cfg.scanner.probe_pdf_pages)
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
            is_pdf_born_digital=is_pdf_bd,
            state="quick_hashed",
            error_code=None,
            error_msg=None,
            last_seen_at=datetime.utcnow().isoformat()+"Z",
        )

    ok_rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=cfg.scanner.max_workers) as ex:
        futs = [ex.submit(process, p) for p in files_to_process]
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                ok_rows.append(fut.result())
            except Exception as e:
                ok_rows.append(dict(path_abs="", state="error", error_code="process", error_msg=str(e)))
            if i % 1000 == 0 or i == total:
                print(f"[PROGRESS] {i}/{total} files enumerated")

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

    batch_size = 1000
    for i in range(0, len(ok_rows), batch_size):
        insert_batch(ok_rows[i:i+batch_size])
        print(f"[DB] inserted {min(i+batch_size, len(ok_rows))}/{len(ok_rows)}")

    # Compute sha256 for groups that look like duplicates (same size + quick_hash)
    cur.execute(
        "SELECT size_bytes, quick_hash, COUNT(*) AS n FROM files "
        "WHERE scan_run_id = ? GROUP BY size_bytes, quick_hash HAVING n > 1",
        (scan_run_id,),
    )
    groups = cur.fetchall()
    print(f"[INFO] duplicate candidate groups this run: {len(groups)}")

    for size, qh, n in groups:
        cur.execute(
            "SELECT file_id, path_abs FROM files WHERE scan_run_id = ? AND size_bytes = ? AND quick_hash = ? AND sha256 IS NULL",
            (scan_run_id, size, qh),
        )
        rows = cur.fetchall()
        for file_id, path_abs in rows:
            try:
                digest = sha256_file(Path(path_abs))
                cur.execute("UPDATE files SET sha256=?, state='done' WHERE file_id=?", (digest, file_id))
            except Exception as e:
                cur.execute("UPDATE files SET state='error', error_code='sha256', error_msg=? WHERE file_id=?", (str(e), file_id))
        con.commit()

    # Mark remaining as done
    cur.execute("UPDATE files SET state='done' WHERE scan_run_id=? AND state='quick_hashed'", (scan_run_id,))
    con.commit()
    con.close()
    print("[DONE] scan complete.")

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
