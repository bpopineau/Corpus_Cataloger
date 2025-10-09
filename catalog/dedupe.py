# catalog/dedupe.py
"""
Duplicate detection module using two-stage hashing:
1. Quick hash (head + tail + size) for fast pre-filtering
2. Full SHA256 for cryptographic verification of potential duplicates
"""
from __future__ import annotations
import argparse, signal, sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import CatalogConfig, load_config
from .db import connect, migrate
from .util import quick_hash, sha256_file

# Callback type aliases (kept local for loose coupling)
ProgressCallback = Callable[[str, int, int, str], None]
LogCallback = Callable[[str], None]
def detect_duplicates(
    cfg: CatalogConfig,
    progress_cb: Optional[ProgressCallback] = None,
    log_cb: Optional[LogCallback] = None,
    enable_quick_hash: bool = True,
    enable_sha256: bool = True,
    max_workers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Run duplicate detection on files in the database.

    Returns statistics about the operation:
    - files_processed: Number of files hashed
    - files_missing: Number of files not found on disk
    - files_error: Number of files with hashing errors
    - quick_hash_count: Files with quick hash computed
    - sha256_count: Files with SHA256 computed
    - duplicate_groups: Number of duplicate file groups found
    - duplicate_files: Total number of duplicate files
    """

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

    workers = max_workers or cfg.dedupe.max_workers or cfg.scanner.max_workers
    small_file_threshold = cfg.dedupe.small_file_threshold
    min_file_size = cfg.dedupe.min_file_size
    min_duplicate_count = cfg.dedupe.min_duplicate_count
    quick_hash_bytes = cfg.dedupe.quick_hash_bytes
    sha_chunk_bytes = cfg.dedupe.sha_chunk_bytes

    emit_progress("start", 0, 0, "Initializing duplicate detection...")
    emit_log(
        "[DEDUPE] Starting duplicate detection with "
        f"{workers} workers | quick_bytes={quick_hash_bytes:,} | sha_chunk={sha_chunk_bytes:,}"
    )
    emit_log(
        f"[DEDUPE] Filters: min_size={min_file_size:,} bytes | min_group={min_duplicate_count} files"
    )

    db_path = Path(cfg.db.path)
    if not db_path.exists():
        emit_log(f"[ERROR] Database not found: {db_path}")
        emit_progress("error", 0, 0, "Database not found")
        return {"error": "Database not found"}

    con = connect(db_path)
    stats: Dict[str, Any] = {
        "files_processed": 0,
        "files_missing": 0,
        "files_error": 0,
        "quick_hash_count": 0,
        "sha256_count": 0,
        "duplicate_groups": 0,
        "duplicate_files": 0,
    }

    cancelled = {"flag": False}

    def _handle_sigint(signum, frame):  # noqa: ARG001
        cancelled["flag"] = True

    try:
        signal.signal(signal.SIGINT, _handle_sigint)
    except Exception:
        pass

    try:
        try:
            con.set_progress_handler(lambda: 1 if cancelled["flag"] else 0, 10000)
        except Exception:
            pass
        migrate(con)
        cur = con.cursor()

        # High-level counts
        cur.execute(
            "SELECT COUNT(*) FROM files WHERE state NOT IN ('error', 'missing')"
        )
        total_active = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COALESCE(SUM(cnt), 0) AS duplicate_population, COUNT(*) AS duplicate_groups
            FROM (
                SELECT COUNT(*) AS cnt
                FROM files
                WHERE state NOT IN ('error', 'missing')
                  AND size_bytes >= ?
                GROUP BY size_bytes, COALESCE(ext, '')
                HAVING COUNT(*) >= ?
            ) AS grouped
            """,
            (min_file_size, min_duplicate_count),
        )
        duplicate_population, duplicate_size_groups = cur.fetchone()
        skipped_singletons = max(0, total_active - duplicate_population)
        emit_log(
            f"[INFO] Active files: {total_active:,} | duplicate size groups: {duplicate_size_groups:,}"
        )
        emit_log(
            f"[INFO] Targeting {duplicate_population:,} files across shared sizes; skipping ~{skipped_singletons:,} singleton files"
        )

        # Stage 1: Quick Hash
        if enable_quick_hash:
            emit_log("[STAGE 1] Quick hash generation")
            emit_progress("quick_hash", 0, 0, "Finding candidates for quick hashing...")

            cur.execute("DROP TABLE IF EXISTS qh_candidates;")
            cur.execute(
                """
                CREATE TEMP TABLE qh_candidates AS
                WITH dup_candidates AS (
                    SELECT size_bytes, COALESCE(ext, '') AS ext
                    FROM files
                    WHERE state NOT IN ('error', 'missing')
                      AND size_bytes >= ?
                    GROUP BY size_bytes, COALESCE(ext, '')
                    HAVING COUNT(*) >= ?
                )
                SELECT f.file_id, f.path_abs, f.size_bytes
                FROM files f
                INNER JOIN dup_candidates dc
                  ON f.size_bytes = dc.size_bytes
                 AND COALESCE(f.ext, '') = dc.ext
                WHERE f.quick_hash IS NULL
                  AND f.state NOT IN ('error', 'missing')
                  AND f.size_bytes >= ?
                ;
                """,
                (min_file_size, min_duplicate_count, small_file_threshold),
            )
            cur.execute("SELECT COUNT(*) FROM qh_candidates")
            total_candidates = cur.fetchone()[0]
            emit_log(
                f"[INFO] Found {total_candidates:,} files needing quick hash"
            )
            emit_progress(
                "quick_hash", 0, total_candidates, f"Processing {total_candidates:,} files"
            )

            def compute_quick_hash(row: Tuple[int, str, int]) -> Dict[str, Any]:
                file_id, path_abs, size_bytes = row
                path = Path(path_abs)
                if not path.exists():
                    return {"file_id": file_id, "status": "missing", "error": "File not found on disk"}
                try:
                    qh = quick_hash(path, quick_hash_bytes)
                    return {"file_id": file_id, "status": "success", "quick_hash": qh}
                except Exception as e:
                    return {"file_id": file_id, "status": "error", "error": str(e)}

            processed = 0
            batch_updates: List[Tuple[str, str, int]] = []
            batch_size_qh = 500
            start_time = time.time()
            last_log_time = start_time
            PAGE = 10_000
            last_rowid = 0

            with ThreadPoolExecutor(max_workers=workers) as ex:
                while not cancelled["flag"]:
                    cur.execute(
                        "SELECT rowid, file_id, path_abs, size_bytes FROM qh_candidates WHERE rowid > ? ORDER BY rowid LIMIT ?",
                        (last_rowid, PAGE),
                    )
                    page = cur.fetchall()
                    if not page:
                        break
                    futures = {
                        ex.submit(compute_quick_hash, (fid, pth, sz)): rowid
                        for rowid, fid, pth, sz in page
                    }
                    last_rowid = page[-1][0]
                    for fut in as_completed(list(futures.keys())):
                        result = fut.result()
                        processed += 1
                        if result["status"] == "success":
                            batch_updates.append(
                                (result["quick_hash"], "quick_hashed", result["file_id"])
                            )
                            stats["quick_hash_count"] += 1
                        elif result["status"] == "missing":
                            cur.execute(
                                "UPDATE files SET state='missing', error_code='not_found', error_msg=? WHERE file_id=?",
                                (result["error"], result["file_id"]),
                            )
                            stats["files_missing"] += 1
                        else:
                            cur.execute(
                                "UPDATE files SET state='error', error_code='hash_failed', error_msg=? WHERE file_id=?",
                                (result["error"], result["file_id"]),
                            )
                            stats["files_error"] += 1

                        if len(batch_updates) >= batch_size_qh:
                            cur.executemany(
                                "UPDATE files SET quick_hash=?, state=? WHERE file_id=?",
                                batch_updates,
                            )
                            con.commit()
                            batch_updates = []

                        if processed % 100 == 0 or processed == total_candidates:
                            elapsed = time.time() - start_time
                            rate = processed / elapsed if elapsed > 0 else 0
                            remaining = (total_candidates - processed) / rate if rate > 0 else 0
                            eta_mins = remaining / 60
                            emit_progress(
                                "quick_hash",
                                processed,
                                total_candidates,
                                f"Hashed {processed:,}/{total_candidates:,} files ({rate:.1f}/s, ETA {eta_mins:.1f}m)",
                            )
                            now = time.time()
                            if now - last_log_time >= 30 or processed == total_candidates:
                                emit_log(
                                    f"[QUICK] {processed:,}/{total_candidates:,} processed ("
                                    f"{stats['quick_hash_count']:,} ok, {stats['files_missing']:,} missing, {stats['files_error']:,} errors) "
                                    f"| {rate:.1f} files/sec | ETA {eta_mins:.1f} min"
                                )
                                last_log_time = now

                        if cancelled["flag"]:
                            emit_log("[CANCEL] Stopping quick-hash (Ctrl+C)")
                            break

                if batch_updates:
                    cur.executemany(
                        "UPDATE files SET quick_hash=?, state=? WHERE file_id=?",
                        batch_updates,
                    )
                    con.commit()
            emit_log(
                f"[STAGE 1] Complete: {stats['quick_hash_count']:,} files quick-hashed"
            )

        # Stage 2: SHA256 for potential duplicates
        if enable_sha256:
            emit_log("[STAGE 2] SHA256 verification")
            emit_progress("sha256", 0, 0, "Finding potential duplicates...")

            cur.execute("DROP TABLE IF EXISTS sha_candidates;")
            cur.execute(
                """
                CREATE TEMP TABLE sha_candidates AS
                WITH dup_candidates AS (
                    SELECT size_bytes, COALESCE(ext, '') AS ext
                    FROM files
                    WHERE state NOT IN ('error', 'missing')
                      AND size_bytes >= ?
                    GROUP BY size_bytes, COALESCE(ext, '')
                    HAVING COUNT(*) >= ?
                ),
                duplicate_quick_hashes AS (
                    SELECT quick_hash
                    FROM files
                    WHERE quick_hash IS NOT NULL
                    GROUP BY quick_hash
                    HAVING COUNT(*) > 1
                )
                SELECT f.file_id, f.path_abs, f.size_bytes, f.quick_hash
                FROM files f
                INNER JOIN dup_candidates dc
                  ON f.size_bytes = dc.size_bytes
                 AND COALESCE(f.ext, '') = dc.ext
                WHERE f.sha256 IS NULL
                  AND f.state NOT IN ('error', 'missing')
                  AND (
                    f.quick_hash IN (SELECT quick_hash FROM duplicate_quick_hashes)
                    OR (f.size_bytes < ? AND f.quick_hash IS NULL AND f.size_bytes >= ?)
                  )
                ;
                """,
                (min_file_size, min_duplicate_count, small_file_threshold, min_file_size),
            )
            cur.execute("SELECT COUNT(*) FROM sha_candidates")
            total_sha = cur.fetchone()[0]
            emit_log(
                f"[INFO] Found {total_sha:,} files needing SHA256 verification"
            )
            emit_progress(
                "sha256", 0, total_sha, f"Verifying {total_sha:,} potential duplicates"
            )

            def compute_sha256(row: Tuple[int, str, int, Optional[str]]) -> Dict[str, Any]:
                file_id, path_abs, size_bytes, quick_hash_existing = row
                path = Path(path_abs)
                if not path.exists():
                    return {"file_id": file_id, "status": "missing", "error": "File not found on disk"}
                try:
                    if size_bytes < small_file_threshold and not quick_hash_existing:
                        data = path.read_bytes()
                        import hashlib
                        sha = hashlib.sha256(data).hexdigest()
                        try:
                            from .util import xxhash
                        except Exception:
                            xxhash = None  # type: ignore
                        n = quick_hash_bytes
                        h = xxhash.xxh64() if xxhash else hashlib.sha1()
                        h.update(str(size_bytes).encode())
                        head = data[:n]
                        if head:
                            h.update(head)
                        if size_bytes > n:
                            tail = data[-n:]
                            if tail:
                                h.update(tail)
                        qh = h.hexdigest()
                        return {"file_id": file_id, "status": "success", "sha256": sha, "quick_hash": qh}
                    else:
                        sha = sha256_file(path, sha_chunk_bytes)
                        return {"file_id": file_id, "status": "success", "sha256": sha}
                except Exception as e:
                    return {"file_id": file_id, "status": "error", "error": str(e)}

            processed_sha = 0
            batch_sha_updates: List[Tuple[str, str, Optional[str], int]] = []
            batch_size_sha = 500
            sha_start_time = time.time()
            sha_last_log_time = sha_start_time
            PAGE = 5_000
            last_rowid = 0

            with ThreadPoolExecutor(max_workers=workers) as ex:
                while not cancelled["flag"]:
                    cur.execute(
                        "SELECT rowid, file_id, path_abs, size_bytes, quick_hash FROM sha_candidates WHERE rowid > ? ORDER BY rowid LIMIT ?",
                        (last_rowid, PAGE),
                    )
                    page = cur.fetchall()
                    if not page:
                        break
                    futures = {
                        ex.submit(compute_sha256, (fid, pth, sz, qh)): rowid
                        for rowid, fid, pth, sz, qh in page
                    }
                    last_rowid = page[-1][0]
                    for fut in as_completed(list(futures.keys())):
                        result = fut.result()
                        processed_sha += 1
                        if result["status"] == "success":
                            batch_sha_updates.append(
                                (
                                    result["sha256"],
                                    "sha_verified",
                                    result.get("quick_hash"),
                                    result["file_id"],
                                )
                            )
                            stats["sha256_count"] += 1
                        elif result["status"] == "missing":
                            cur.execute(
                                "UPDATE files SET state='missing', error_code='not_found', error_msg=? WHERE file_id=?",
                                (result["error"], result["file_id"]),
                            )
                            stats["files_missing"] += 1
                        else:
                            cur.execute(
                                "UPDATE files SET state='error', error_code='hash_failed', error_msg=? WHERE file_id=?",
                                (result["error"], result["file_id"]),
                            )
                            stats["files_error"] += 1

                        if len(batch_sha_updates) >= batch_size_sha:
                            cur.executemany(
                                "UPDATE files SET sha256=?, state=?, quick_hash=COALESCE(quick_hash, ?) WHERE file_id=?",
                                batch_sha_updates,
                            )
                            con.commit()
                            batch_sha_updates = []

                        if processed_sha % 50 == 0 or processed_sha == total_sha:
                            sha_elapsed = time.time() - sha_start_time
                            sha_rate = processed_sha / sha_elapsed if sha_elapsed > 0 else 0
                            sha_remaining = (total_sha - processed_sha) / sha_rate if sha_rate > 0 else 0
                            sha_eta_mins = sha_remaining / 60
                            emit_progress(
                                "sha256",
                                processed_sha,
                                total_sha,
                                f"Verified {processed_sha:,}/{total_sha:,} files ({sha_rate:.1f}/s, ETA {sha_eta_mins:.1f}m)",
                            )
                            now = time.time()
                            if now - sha_last_log_time >= 30 or processed_sha == total_sha:
                                emit_log(
                                    f"[SHA256] {processed_sha:,}/{total_sha:,} processed | {sha_rate:.1f} files/sec | ETA {sha_eta_mins:.1f} min"
                                )
                                sha_last_log_time = now

                        if cancelled["flag"]:
                            emit_log("[CANCEL] Stopping SHA256 (Ctrl+C)")
                            break

                if batch_sha_updates:
                    cur.executemany(
                        "UPDATE files SET sha256=?, state=?, quick_hash=COALESCE(quick_hash, ?) WHERE file_id=?",
                        batch_sha_updates,
                    )
                    con.commit()
            emit_log(
                f"[STAGE 2] Complete: {stats['sha256_count']:,} files verified"
            )

        # Stage 3: Identify duplicates
        emit_progress("analyze", 0, 0, "Analyzing duplicates...")
        emit_log("[STAGE 3] Analyzing duplicate groups")
        cur.execute(
            """
            SELECT sha256, COUNT(*) as count, SUM(size_bytes) as total_size
            FROM files
            WHERE sha256 IS NOT NULL
              AND state NOT IN ('error', 'missing')
            GROUP BY sha256
            HAVING COUNT(*) > 1
            ORDER BY total_size DESC
            """
        )
        duplicate_groups = cur.fetchall()
        stats["duplicate_groups"] = len(duplicate_groups)
        stats["duplicate_files"] = sum(row[1] for row in duplicate_groups)
        total_wasted_bytes = sum(row[2] - (row[2] / row[1]) for row in duplicate_groups)
        emit_log(f"[RESULT] Found {stats['duplicate_groups']:,} duplicate groups")
        emit_log(f"[RESULT] {stats['duplicate_files']:,} duplicate files")
        emit_log(f"[RESULT] ~{total_wasted_bytes / (1024**3):.2f} GB wasted space")

        # Mark remaining files as done
        cur.execute(
            """
            UPDATE files 
            SET state='done' 
            WHERE state IN ('quick_hashed', 'sha_verified')
              AND sha256 IS NOT NULL
            """
        )
        con.commit()

        stats["files_processed"] = stats["quick_hash_count"] + stats["sha256_count"]
        emit_progress("done", stats["files_processed"], stats["files_processed"], "Duplicate detection complete")
        emit_log("[DONE] Duplicate detection complete")
        return stats

    finally:
        try:
            con.close()
        except Exception:
            pass


def get_duplicate_report(db_path: Path, limit: int = 100) -> List[Dict]:
    """
    Get a report of duplicate files with details.
    
    Returns list of duplicate groups with:
    - sha256: The hash
    - count: Number of duplicates
    - size_bytes: Size of each file
    - total_wasted: Wasted space (total - one copy)
    - paths: List of file paths
    """
    con = connect(db_path)
    try:
        cur = con.cursor()
        
        cur.execute("""
            SELECT sha256, COUNT(*) as count, size_bytes
            FROM files
            WHERE sha256 IS NOT NULL
              AND state NOT IN ('error', 'missing')
            GROUP BY sha256
            HAVING COUNT(*) > 1
            ORDER BY size_bytes * (COUNT(*) - 1) DESC
            LIMIT ?
        """, (limit,))
        
        results = []
        for row in cur.fetchall():
            sha256, count, size_bytes = row
            
            # Get all paths for this duplicate group
            cur.execute("""
                SELECT path_abs, mtime_utc
                FROM files
                WHERE sha256 = ?
                ORDER BY mtime_utc ASC
            """, (sha256,))
            
            paths = [{"path": p[0], "mtime": p[1]} for p in cur.fetchall()]
            
            results.append({
                "sha256": sha256,
                "count": count,
                "size_bytes": size_bytes,
                "total_wasted": size_bytes * (count - 1),
                "paths": paths
            })
        
        return results
        
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser(description="Corpus Cataloger - Duplicate Detection")
    ap.add_argument("--config", required=True, help="Path to config file")
    ap.add_argument("--max-workers", type=int, default=None, help="Number of worker threads")
    ap.add_argument("--skip-quick-hash", action="store_true", help="Skip quick hash stage")
    ap.add_argument("--skip-sha256", action="store_true", help="Skip SHA256 stage")
    ap.add_argument("--report", action="store_true", help="Show duplicate report after detection")
    ap.add_argument("--report-only", action="store_true", help="Only show report, skip detection")
    ap.add_argument("--report-limit", type=int, default=100, help="Limit number of duplicate groups in report")
    args = ap.parse_args()
    
    cfg = load_config(Path(args.config))
    
    if not args.report_only:
        stats = detect_duplicates(
            cfg,
            enable_quick_hash=not args.skip_quick_hash,
            enable_sha256=not args.skip_sha256,
            max_workers=args.max_workers
        )
        
        print("\n" + "=" * 70)
        print("DUPLICATE DETECTION SUMMARY")
        print("=" * 70)
        print(f"Files processed:       {stats['files_processed']:>10,}")
        print(f"Quick hashes:          {stats['quick_hash_count']:>10,}")
        print(f"SHA256 hashes:         {stats['sha256_count']:>10,}")
        print(f"Files missing:         {stats['files_missing']:>10,}")
        print(f"Files with errors:     {stats['files_error']:>10,}")
        print(f"Duplicate groups:      {stats['duplicate_groups']:>10,}")
        print(f"Total duplicate files: {stats['duplicate_files']:>10,}")
        print("=" * 70)
    
    if args.report or args.report_only:
        print("\n" + "=" * 70)
        print(f"TOP {args.report_limit} DUPLICATE GROUPS (by wasted space)")
        print("=" * 70)
        
        report = get_duplicate_report(Path(cfg.db.path), args.report_limit)
        
        for i, group in enumerate(report, 1):
            wasted_mb = group["total_wasted"] / (1024**2)
            size_mb = group["size_bytes"] / (1024**2)
            print(f"\n#{i} - {group['count']} copies × {size_mb:.2f} MB = {wasted_mb:.2f} MB wasted")
            print(f"    SHA256: {group['sha256'][:16]}...")
            print(f"    Files:")
            for path_info in group["paths"]:
                print(f"      - {path_info['path']}")
                print(f"        Modified: {path_info['mtime']}")


if __name__ == "__main__":
    main()
