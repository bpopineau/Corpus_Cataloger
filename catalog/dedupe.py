# catalog/dedupe.py
"""
Duplicate detection module using two-stage hashing:
1. Quick hash (head + tail + size) for fast pre-filtering
2. Full SHA256 for cryptographic verification of potential duplicates
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import load_config, CatalogConfig
from .db import connect, migrate
from .util import quick_hash, sha256_file

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
    quick_hash_bytes = cfg.dedupe.quick_hash_bytes
    sha_chunk_bytes = cfg.dedupe.sha_chunk_bytes
    
    emit_progress("start", 0, 0, "Initializing duplicate detection...")
    emit_log(
        "[DEDUPE] Starting duplicate detection with "
        f"{workers} workers | quick_bytes={quick_hash_bytes:,} | sha_chunk={sha_chunk_bytes:,}"
    )
    emit_log(f"[DEDUPE] Skipping quick hash for files smaller than {small_file_threshold:,} bytes")
    
    db_path = Path(cfg.db.path)
    if not db_path.exists():
        emit_log(f"[ERROR] Database not found: {db_path}")
        emit_progress("error", 0, 0, "Database not found")
        return {"error": "Database not found"}
    
    con = connect(db_path)
    stats = {
        "files_processed": 0,
        "files_missing": 0,
        "files_error": 0,
        "quick_hash_count": 0,
        "sha256_count": 0,
        "duplicate_groups": 0,
        "duplicate_files": 0,
    }
    
    try:
        migrate(con)
        cur = con.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM files
            WHERE state NOT IN ('error', 'missing')
        """)
        total_active = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COALESCE(SUM(cnt), 0) AS duplicate_population,
                COUNT(*) AS duplicate_groups
            FROM (
                SELECT COUNT(*) AS cnt
                FROM files
                WHERE state NOT IN ('error', 'missing')
                GROUP BY size_bytes, COALESCE(ext, '')
                HAVING COUNT(*) > 1
            ) AS grouped
        """)
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

            # Find files that need quick hashing (only files sharing sizes/extensions)
            cur.execute("""
                WITH dup_candidates AS (
                    SELECT size_bytes, COALESCE(ext, '') AS ext
                    FROM files
                    WHERE state NOT IN ('error', 'missing')
                    GROUP BY size_bytes, COALESCE(ext, '')
                    HAVING COUNT(*) > 1
                )
                SELECT f.file_id, f.path_abs, f.size_bytes
                FROM files f
                INNER JOIN dup_candidates dc
                  ON f.size_bytes = dc.size_bytes
                 AND COALESCE(f.ext, '') = dc.ext
                WHERE f.quick_hash IS NULL
                  AND f.state NOT IN ('error', 'missing')
                  AND f.size_bytes >= ?
                ORDER BY f.size_bytes DESC
            """, (small_file_threshold,))
            
            candidates = cur.fetchall()
            total_candidates = len(candidates)
            
            emit_log(f"[INFO] Found {total_candidates:,} files needing quick hash")
            emit_progress("quick_hash", 0, total_candidates, f"Processing {total_candidates:,} files")
            
            def compute_quick_hash(row: Tuple) -> Dict:
                file_id, path_abs, size_bytes = row
                path = Path(path_abs)
                
                # Check if file still exists
                if not path.exists():
                    return {
                        "file_id": file_id,
                        "status": "missing",
                        "error": "File not found on disk"
                    }
                
                try:
                    qh = quick_hash(path, quick_hash_bytes)
                    return {
                        "file_id": file_id,
                        "status": "success",
                        "quick_hash": qh
                    }
                except Exception as e:
                    return {
                        "file_id": file_id,
                        "status": "error",
                        "error": str(e)
                    }
            
            # Process in parallel
            processed = 0
            batch_updates = []
            batch_size = 500
            
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(compute_quick_hash, row): row for row in candidates}
                
                for fut in as_completed(futures):
                    result = fut.result()
                    processed += 1
                    
                    if result["status"] == "success":
                        batch_updates.append((
                            result["quick_hash"],
                            "quick_hashed",
                            result["file_id"]
                        ))
                        stats["quick_hash_count"] += 1
                    elif result["status"] == "missing":
                        cur.execute("""
                            UPDATE files 
                            SET state='missing', error_code='not_found', error_msg=?
                            WHERE file_id=?
                        """, (result["error"], result["file_id"]))
                        stats["files_missing"] += 1
                    else:
                        cur.execute("""
                            UPDATE files 
                            SET state='error', error_code='hash_failed', error_msg=?
                            WHERE file_id=?
                        """, (result["error"], result["file_id"]))
                        stats["files_error"] += 1
                    
                    # Batch update quick hashes
                    if len(batch_updates) >= batch_size:
                        cur.executemany("""
                            UPDATE files 
                            SET quick_hash=?, state=?
                            WHERE file_id=?
                        """, batch_updates)
                        con.commit()
                        batch_updates = []
                    
                    if processed % 100 == 0 or processed == total_candidates:
                        emit_progress("quick_hash", processed, total_candidates, 
                                    f"Hashed {processed:,}/{total_candidates:,} files")
                        emit_log(f"[QUICK] {processed:,}/{total_candidates:,} processed "
                               f"({stats['quick_hash_count']:,} ok, "
                               f"{stats['files_missing']:,} missing, "
                               f"{stats['files_error']:,} errors)")
            
            # Final batch commit
            if batch_updates:
                cur.executemany("""
                    UPDATE files 
                    SET quick_hash=?, state=?
                    WHERE file_id=?
                """, batch_updates)
                con.commit()
            
            emit_log(f"[STAGE 1] Complete: {stats['quick_hash_count']:,} files quick-hashed")
        
        # Stage 2: SHA256 for potential duplicates
        if enable_sha256:
            emit_log("[STAGE 2] SHA256 verification")
            emit_progress("sha256", 0, 0, "Finding potential duplicates...")

            # Find files that have matching quick_hash OR are small files needing SHA256
            cur.execute("""
                WITH dup_candidates AS (
                    SELECT size_bytes, COALESCE(ext, '') AS ext
                    FROM files
                    WHERE state NOT IN ('error', 'missing')
                    GROUP BY size_bytes, COALESCE(ext, '')
                    HAVING COUNT(*) > 1
                ),
                duplicate_quick_hashes AS (
                    SELECT quick_hash
                    FROM files
                    WHERE quick_hash IS NOT NULL
                    GROUP BY quick_hash
                    HAVING COUNT(*) > 1
                )
                SELECT f.file_id, f.path_abs, f.size_bytes
                FROM files f
                INNER JOIN dup_candidates dc
                  ON f.size_bytes = dc.size_bytes
                 AND COALESCE(f.ext, '') = dc.ext
                WHERE f.sha256 IS NULL
                  AND f.state NOT IN ('error', 'missing')
                  AND (
                    f.quick_hash IN (SELECT quick_hash FROM duplicate_quick_hashes)
                    OR (f.size_bytes < ? AND f.quick_hash IS NULL)
                  )
                ORDER BY f.size_bytes DESC
            """, (small_file_threshold,))
            
            sha_candidates = cur.fetchall()
            total_sha = len(sha_candidates)
            
            emit_log(f"[INFO] Found {total_sha:,} files needing SHA256 verification")
            emit_progress("sha256", 0, total_sha, f"Verifying {total_sha:,} potential duplicates")
            
            def compute_sha256(row: Tuple) -> Dict:
                file_id, path_abs, size_bytes = row
                path = Path(path_abs)
                
                if not path.exists():
                    return {
                        "file_id": file_id,
                        "status": "missing",
                        "error": "File not found on disk"
                    }
                
                try:
                    sha = sha256_file(path, sha_chunk_bytes)
                    return {
                        "file_id": file_id,
                        "status": "success",
                        "sha256": sha
                    }
                except Exception as e:
                    return {
                        "file_id": file_id,
                        "status": "error",
                        "error": str(e)
                    }
            
            processed_sha = 0
            batch_sha_updates = []
            
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(compute_sha256, row): row for row in sha_candidates}
                
                for fut in as_completed(futures):
                    result = fut.result()
                    processed_sha += 1
                    
                    if result["status"] == "success":
                        batch_sha_updates.append((
                            result["sha256"],
                            "sha_verified",
                            result["file_id"]
                        ))
                        stats["sha256_count"] += 1
                    elif result["status"] == "missing":
                        cur.execute("""
                            UPDATE files 
                            SET state='missing', error_code='not_found', error_msg=?
                            WHERE file_id=?
                        """, (result["error"], result["file_id"]))
                        stats["files_missing"] += 1
                    else:
                        cur.execute("""
                            UPDATE files 
                            SET state='error', error_code='hash_failed', error_msg=?
                            WHERE file_id=?
                        """, (result["error"], result["file_id"]))
                        stats["files_error"] += 1
                    
                    if len(batch_sha_updates) >= batch_size:
                        cur.executemany("""
                            UPDATE files 
                            SET sha256=?, state=?
                            WHERE file_id=?
                        """, batch_sha_updates)
                        con.commit()
                        batch_sha_updates = []
                    
                    if processed_sha % 50 == 0 or processed_sha == total_sha:
                        emit_progress("sha256", processed_sha, total_sha,
                                    f"Verified {processed_sha:,}/{total_sha:,} files")
                        emit_log(f"[SHA256] {processed_sha:,}/{total_sha:,} processed")
            
            if batch_sha_updates:
                cur.executemany("""
                    UPDATE files 
                    SET sha256=?, state=?
                    WHERE file_id=?
                """, batch_sha_updates)
                con.commit()
            
            emit_log(f"[STAGE 2] Complete: {stats['sha256_count']:,} files verified")
        
        # Stage 3: Identify duplicates
        emit_progress("analyze", 0, 0, "Analyzing duplicates...")
        emit_log("[STAGE 3] Analyzing duplicate groups")
        
        cur.execute("""
            SELECT sha256, COUNT(*) as count, SUM(size_bytes) as total_size
            FROM files
            WHERE sha256 IS NOT NULL
              AND state NOT IN ('error', 'missing')
            GROUP BY sha256
            HAVING COUNT(*) > 1
            ORDER BY total_size DESC
        """)
        
        duplicate_groups = cur.fetchall()
        stats["duplicate_groups"] = len(duplicate_groups)
        stats["duplicate_files"] = sum(row[1] for row in duplicate_groups)
        
        total_wasted_bytes = sum(row[2] - (row[2] / row[1]) for row in duplicate_groups)
        
        emit_log(f"[RESULT] Found {stats['duplicate_groups']:,} duplicate groups")
        emit_log(f"[RESULT] {stats['duplicate_files']:,} duplicate files")
        emit_log(f"[RESULT] ~{total_wasted_bytes / (1024**3):.2f} GB wasted space")
        
        # Mark remaining files as done
        cur.execute("""
            UPDATE files 
            SET state='done' 
            WHERE state IN ('quick_hashed', 'sha_verified')
              AND sha256 IS NOT NULL
        """)
        con.commit()
        
        stats["files_processed"] = stats["quick_hash_count"] + stats["sha256_count"]
        
        emit_progress("done", stats["files_processed"], stats["files_processed"], 
                     "Duplicate detection complete")
        emit_log("[DONE] Duplicate detection complete")
        
        return stats
        
    finally:
        con.close()


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
