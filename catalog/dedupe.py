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
try:
    import blake3  # type: ignore
    HAS_BLAKE3 = True
except Exception:
    HAS_BLAKE3 = False

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
    network_friendly: bool = False,
    include_prefixes: Optional[List[str]] = None,
    exclude_prefixes: Optional[List[str]] = None,
    progressive: bool = False,
    sample_bytes: Optional[int] = None,
    io_bytes_per_sec: Optional[int] = None,
    use_blake3: bool = False,
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
    if sample_bytes is None:
        sample_bytes = min(cfg.dedupe.quick_hash_bytes, 64 * 1024)

    # Network-friendly mode reduces read sizes and concurrency bursts
    if network_friendly:
        quick_hash_bytes = min(quick_hash_bytes, 64 * 1024)  # cap at 64 KiB
        sha_chunk_bytes = min(sha_chunk_bytes, 256 * 1024)   # cap at 256 KiB
        if max_workers is None:
            workers = min(workers, 2)
        # Also cap sampling size to keep reads small
        sample_bytes = min(sample_bytes or 64 * 1024, 64 * 1024)

    # Build optional path filters
    include_prefixes = include_prefixes or []
    exclude_prefixes = exclude_prefixes or []

    def path_filter_sql(alias: str) -> Tuple[str, List[str]]:
        clauses: List[str] = []
        params: List[str] = []
        if include_prefixes:
            ors = []
            for p in include_prefixes:
                ors.append(f"{alias}.path_abs LIKE ?")
                params.append(p.rstrip('\\/') + '%')
            clauses.append('(' + ' OR '.join(ors) + ')')
        if exclude_prefixes:
            ors = []
            for p in exclude_prefixes:
                ors.append(f"{alias}.path_abs LIKE ?")
                params.append(p.rstrip('\\/') + '%')
            clauses.append('NOT (' + ' OR '.join(ors) + ')')
        if clauses:
            return ' AND ' + ' AND '.join(clauses) + ' ', params
        return '', []

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
            filt_sql, filt_params = path_filter_sql('f')
            qh_sql = (
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
                """
                + filt_sql +
                ";"
            )
            cur.execute(qh_sql, (min_file_size, min_duplicate_count, min_file_size, *filt_params))
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

            if progressive:
                # Progressive staged sampling: head hash (h1), then tail hash (h2), then full sha for remaining collisions
                emit_log("[PROG] Progressive mode enabled")
                cur.execute("DROP TABLE IF EXISTS prog_candidates;")
                filt_sql, filt_params = path_filter_sql('f')
                # Bring over persisted h1/h2 to avoid recomputation when unchanged
                cur.execute(
                    (
                        """
                        CREATE TEMP TABLE prog_candidates AS
                        WITH dup_candidates AS (
                            SELECT size_bytes, COALESCE(ext, '') AS ext
                            FROM files
                            WHERE state NOT IN ('error', 'missing')
                              AND size_bytes >= ?
                            GROUP BY size_bytes, COALESCE(ext, '')
                            HAVING COUNT(*) >= ?
                        )
                        SELECT f.file_id, f.path_abs, f.size_bytes,
                               f.h1 AS h1, f.h2 AS h2, f.mtime_utc
                        FROM files f
                        INNER JOIN dup_candidates dc
                          ON f.size_bytes = dc.size_bytes
                         AND COALESCE(f.ext, '') = dc.ext
                        WHERE f.sha256 IS NULL
                          AND f.state NOT IN ('error', 'missing')
                        """
                        + filt_sql +
                        ";"
                    ),
                    (min_file_size, min_duplicate_count, *filt_params),
                )

                def _h_algo():
                    try:
                        from .util import xxhash  # type: ignore
                        return 'xxhash'
                    except Exception:
                        return 'blake2b'

                import hashlib

                def hash_sample_head(path: Path, k: int) -> str:
                    try:
                        data = b""
                        with path.open('rb', buffering=1024*64) as f:
                            data = f.read(k)
                        if io_bytes_per_sec and len(data) > 0:
                            time.sleep(len(data) / float(io_bytes_per_sec))
                        if _h_algo() == 'xxhash':
                            from .util import xxhash  # type: ignore
                            h = xxhash.xxh64()
                            h.update(data)
                            return h.hexdigest()
                        else:
                            return hashlib.blake2b(data, digest_size=16).hexdigest()
                    except Exception as e:
                        raise e

                def hash_sample_tail(path: Path, k: int, size_bytes: int) -> str:
                    try:
                        read = min(k, max(0, size_bytes))
                        if read == 0:
                            return ""
                        with path.open('rb', buffering=1024*64) as f:
                            # Seek to tail start
                            start = max(0, size_bytes - read)
                            f.seek(start)
                            data = f.read(read)
                        if io_bytes_per_sec and len(data) > 0:
                            time.sleep(len(data) / float(io_bytes_per_sec))
                        if _h_algo() == 'xxhash':
                            from .util import xxhash  # type: ignore
                            h = xxhash.xxh64()
                            h.update(data)
                            return h.hexdigest()
                        else:
                            return hashlib.blake2b(data, digest_size=16).hexdigest()
                    except Exception as e:
                        raise e

                # Stage 2.1: compute h1 (head hash) for all prog candidates
                cur.execute("SELECT COUNT(*) FROM prog_candidates")
                total_prog = cur.fetchone()[0]
                emit_log(f"[PROG] Candidates: {total_prog:,}")
                emit_progress("sha256", 0, total_prog, "Sampling file heads (h1)...")

                PAGE = 10_000
                last_rowid = 0
                processed_h1 = 0
                batch_h1_updates: List[Tuple[str, int]] = []

                def compute_h1(rowid: int, file_id: int, path_abs: str) -> Tuple[int, int, str]:
                    h1 = hash_sample_head(Path(path_abs), sample_bytes or 32768)
                    return rowid, file_id, h1

                with ThreadPoolExecutor(max_workers=workers) as ex:
                    while not cancelled["flag"]:
                        cur.execute(
                            "SELECT rowid, file_id, path_abs FROM prog_candidates WHERE rowid > ? AND h1 IS NULL ORDER BY rowid LIMIT ?",
                            (last_rowid, PAGE),
                        )
                        page = cur.fetchall()
                        if not page:
                            break
                        h1_futures = [
                            ex.submit(compute_h1, rowid, fid, pth)
                            for rowid, fid, pth in page
                        ]
                        last_rowid = page[-1][0]
                        for fut_h1 in as_completed(h1_futures):
                            rowid, file_id, h1 = fut_h1.result()
                            processed_h1 += 1
                            batch_h1_updates.append((h1, int(file_id)))
                            if len(batch_h1_updates) >= 1000:
                                cur.executemany("UPDATE prog_candidates SET h1=? WHERE file_id=?", batch_h1_updates)
                                con.commit()
                                batch_h1_updates = []
                            if processed_h1 % 200 == 0 or processed_h1 == total_prog:
                                emit_progress("sha256", processed_h1, total_prog, "Sampling file heads (h1)...")
                            if cancelled["flag"]:
                                break
                if batch_h1_updates:
                    cur.executemany("UPDATE prog_candidates SET h1=? WHERE file_id=?", batch_h1_updates)
                    con.commit()

                # Stage 2.2: compute h2 (tail hash) only for collisions on (size, h1)
                emit_progress("sha256", 0, 0, "Sampling file tails (h2) for collisions...")
                cur.execute("DROP TABLE IF EXISTS h1_collisions;")
                cur.execute(
                    """
                    CREATE TEMP TABLE h1_collisions AS
                    SELECT p.file_id, p.path_abs, p.size_bytes
                    FROM prog_candidates p
                    JOIN (
                        SELECT size_bytes, h1
                        FROM prog_candidates
                        WHERE h1 IS NOT NULL
                        GROUP BY size_bytes, h1
                        HAVING COUNT(*) > 1
                    ) g
                    ON p.size_bytes = g.size_bytes AND p.h1 = g.h1
                    """
                )
                cur.execute("SELECT COUNT(*) FROM h1_collisions")
                total_h1_col = cur.fetchone()[0]
                emit_log(f"[PROG] H1 collision candidates: {total_h1_col:,}")
                PAGE = 10_000
                last_rowid = 0
                processed_h2 = 0
                batch_h2_updates: List[Tuple[str, int]] = []

                def compute_h2(rowid: int, file_id: int, path_abs: str, size_bytes_val: int) -> Tuple[int, int, str]:
                    h2 = hash_sample_tail(Path(path_abs), sample_bytes or 32768, size_bytes_val)
                    return rowid, file_id, h2

                with ThreadPoolExecutor(max_workers=workers) as ex:
                    while not cancelled["flag"]:
                        cur.execute(
                            "SELECT rowid, file_id, path_abs, size_bytes FROM h1_collisions WHERE rowid > ? ORDER BY rowid LIMIT ?",
                            (last_rowid, PAGE),
                        )
                        page = cur.fetchall()
                        if not page:
                            break
                        h2_futures = [
                            ex.submit(compute_h2, rowid, fid, pth, sz)
                            for rowid, fid, pth, sz in page
                        ]
                        last_rowid = page[-1][0]
                        for fut_h2 in as_completed(h2_futures):
                            rowid, file_id, h2 = fut_h2.result()
                            processed_h2 += 1
                            batch_h2_updates.append((h2, int(file_id)))
                            if len(batch_h2_updates) >= 1000:
                                cur.executemany("UPDATE prog_candidates SET h2=? WHERE file_id=?", batch_h2_updates)
                                con.commit()
                                batch_h2_updates = []
                            if processed_h2 % 200 == 0 or processed_h2 == total_h1_col:
                                emit_progress("sha256", processed_h2, total_h1_col, "Sampling file tails (h2) for collisions...")
                            if cancelled["flag"]:
                                break
                if batch_h2_updates:
                    cur.executemany("UPDATE prog_candidates SET h2=? WHERE file_id=?", batch_h2_updates)
                    con.commit()

                # Persist h1/h2 to files for reuse in future runs
                cur.execute(
                    "UPDATE files SET h1=(SELECT h1 FROM prog_candidates WHERE prog_candidates.file_id=files.file_id) WHERE file_id IN (SELECT file_id FROM prog_candidates WHERE h1 IS NOT NULL)"
                )
                cur.execute(
                    "UPDATE files SET h2=(SELECT h2 FROM prog_candidates WHERE prog_candidates.file_id=files.file_id) WHERE file_id IN (SELECT file_id FROM prog_candidates WHERE h2 IS NOT NULL)"
                )
                con.commit()

                # Stage 2.3: derive sha_candidates from (size, h1, h2) groups with count > 1
                cur.execute("DROP TABLE IF EXISTS sha_candidates;")
                cur.execute(
                    """
                    CREATE TEMP TABLE sha_candidates AS
                    SELECT p.file_id, p.path_abs, p.size_bytes, NULL AS quick_hash
                    FROM prog_candidates p
                    JOIN (
                        SELECT size_bytes, h1, h2
                        FROM prog_candidates
                        WHERE h1 IS NOT NULL AND h2 IS NOT NULL
                        GROUP BY size_bytes, h1, h2
                        HAVING COUNT(*) > 1
                    ) g
                    ON p.size_bytes = g.size_bytes AND p.h1 = g.h1 AND p.h2 = g.h2
                    """
                )
                cur.execute("SELECT COUNT(*) FROM sha_candidates")
                total_sha = cur.fetchone()[0]
                emit_log(f"[PROG] Proceeding to full SHA for {total_sha:,} files")
                emit_progress("sha256", 0, total_sha, f"Verifying {total_sha:,} potential duplicates")

            else:
                # Original SHA candidates path (quick-hash centric)
                cur.execute("DROP TABLE IF EXISTS sha_candidates;")
                filt_sql, filt_params = path_filter_sql('f')
                sha_sql = (
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
                    """
                )
                if network_friendly:
                    sha_sql += " AND f.quick_hash IN (SELECT quick_hash FROM duplicate_quick_hashes) "
                    sha_params = [min_file_size, min_duplicate_count]
                else:
                    sha_sql += (
                        " AND ( f.quick_hash IN (SELECT quick_hash FROM duplicate_quick_hashes)"
                        " OR (f.size_bytes < ? AND f.quick_hash IS NULL AND f.size_bytes >= ?) ) "
                    )
                    sha_params = [min_file_size, min_duplicate_count, small_file_threshold, min_file_size]
                sha_sql += filt_sql + ";"
                cur.execute(sha_sql, (*sha_params, *filt_params))
                cur.execute("SELECT COUNT(*) FROM sha_candidates")
                total_sha = cur.fetchone()[0]
                emit_log(
                    f"[INFO] Found {total_sha:,} files needing SHA256 verification"
                )
                emit_progress(
                    "sha256", 0, total_sha, f"Verifying {total_sha:,} potential duplicates"
                )

            def _sha256_file_throttled(path: Path, chunk_size: int, bps: int) -> str:
                import hashlib
                hasher = hashlib.sha256()
                with path.open('rb', buffering=1024*64) as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        time.sleep(len(chunk) / float(bps))
                return hasher.hexdigest()

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
                        if use_blake3 and HAS_BLAKE3:
                            # Compute BLAKE3 and SHA-256 in one pass to avoid extra I/O
                            import hashlib
                            sha_hasher = hashlib.sha256()
                            b3_hasher = blake3.blake3()
                            with path.open('rb', buffering=1024*64) as f:
                                while True:
                                    chunk = f.read(sha_chunk_bytes)
                                    if not chunk:
                                        break
                                    sha_hasher.update(chunk)
                                    b3_hasher.update(chunk)
                                    if io_bytes_per_sec:
                                        time.sleep(len(chunk) / float(io_bytes_per_sec))
                            return {
                                "file_id": file_id,
                                "status": "success",
                                "sha256": sha_hasher.hexdigest(),
                                "blake3": b3_hasher.hexdigest(),
                            }
                        else:
                            if io_bytes_per_sec and io_bytes_per_sec > 0:
                                sha = _sha256_file_throttled(path, sha_chunk_bytes, io_bytes_per_sec)
                            else:
                                sha = sha256_file(path, sha_chunk_bytes)
                            return {"file_id": file_id, "status": "success", "sha256": sha}
                except Exception as e:
                    return {"file_id": file_id, "status": "error", "error": str(e)}

            processed_sha = 0
            batch_sha_updates: List[Tuple[Optional[str], str, Optional[str], Optional[str], int]] = []
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
                                    result.get("sha256"),
                                    "sha_verified",
                                    result.get("quick_hash"),
                                    result.get("blake3"),
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
                                "UPDATE files SET sha256=COALESCE(?, sha256), state=?, quick_hash=COALESCE(quick_hash, ?), blake3=COALESCE(?, blake3) WHERE file_id=?",
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
                        "UPDATE files SET sha256=COALESCE(?, sha256), state=?, quick_hash=COALESCE(quick_hash, ?), blake3=COALESCE(?, blake3) WHERE file_id=?",
                        batch_sha_updates,
                    )
                    con.commit()
            emit_log(
                f"[STAGE 2] Complete: {stats['sha256_count']:,} files verified"
            )

        # Stage 3: Identify duplicates
        emit_progress("analyze", 0, 0, "Analyzing duplicates...")
        emit_log("[STAGE 3] Analyzing duplicate groups")
        filt_sql, filt_params = path_filter_sql('f')
        cur.execute(
            """
            SELECT sha256, COUNT(*) as count, SUM(size_bytes) as total_size
            FROM files f
            WHERE sha256 IS NOT NULL
              AND state NOT IN ('error', 'missing')
            """ + filt_sql + " GROUP BY sha256 HAVING COUNT(*) > 1 ORDER BY total_size DESC",
            (*filt_params,),
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


def get_duplicate_report(
    db_path: Path,
    limit: int = 100,
    include_prefixes: Optional[List[str]] = None,
    exclude_prefixes: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Get a report of duplicate files with details.
    
    Returns list of duplicate groups with:
    - sha256: The hash
    - count: Number of duplicates
    - size_bytes: Size of each file
    - total_wasted: Wasted space (total - one copy)
    - paths: List of file paths
    """
    include_prefixes = include_prefixes or []
    exclude_prefixes = exclude_prefixes or []

    def path_filter_sql(alias: str) -> Tuple[str, List[str]]:
        clauses: List[str] = []
        params: List[str] = []
        if include_prefixes:
            ors = []
            for p in include_prefixes:
                ors.append(f"{alias}.path_abs LIKE ?")
                params.append(p.rstrip('\\/') + '%')
            clauses.append('(' + ' OR '.join(ors) + ')')
        if exclude_prefixes:
            ors = []
            for p in exclude_prefixes:
                ors.append(f"{alias}.path_abs LIKE ?")
                params.append(p.rstrip('\\/') + '%')
            clauses.append('NOT (' + ' OR '.join(ors) + ')')
        if clauses:
            return ' AND ' + ' AND '.join(clauses) + ' ', params
        return '', []

    con = connect(db_path)
    try:
        cur = con.cursor()

        filt_sql, filt_params = path_filter_sql('f')
        cur.execute(
            """
            SELECT sha256, COUNT(*) as count, size_bytes
            FROM files f
            WHERE sha256 IS NOT NULL
              AND state NOT IN ('error', 'missing')
            """
            + filt_sql +
            " GROUP BY sha256 HAVING COUNT(*) > 1 ORDER BY size_bytes * (COUNT(*) - 1) DESC LIMIT ?",
            (*filt_params, limit),
        )
        
        results = []
        for row in cur.fetchall():
            sha256, count, size_bytes = row
            
            # Get all paths for this duplicate group
            path_filt_sql, path_filt_params = path_filter_sql('f')
            cur.execute(
                """
                SELECT path_abs, mtime_utc
                FROM files f
                WHERE sha256 = ?
                """ + path_filt_sql + " ORDER BY mtime_utc ASC",
                (sha256, *path_filt_params),
            )
            
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
    ap.add_argument("--network-friendly", action="store_true", help="Reduce network I/O (lower concurrency, smaller quick-hash window)")
    ap.add_argument("--include-prefix", action="append", default=None, help="Only process files with absolute paths starting with this prefix (can repeat)")
    ap.add_argument("--exclude-prefix", action="append", default=None, help="Skip files with absolute paths starting with this prefix (can repeat)")
    ap.add_argument("--progressive", action="store_true", help="Progressive staged sampling (head/tail) before full SHA; persists h1/h2 in DB")
    ap.add_argument("--sample-bytes", type=int, default=None, help="Bytes to read for head/tail sampling (default: min(quick_hash_bytes, 64KiB))")
    ap.add_argument("--io-bytes-per-sec", type=int, default=None, help="Throttle file reading to this many bytes/sec (approximate)")
    ap.add_argument("--blake3", action="store_true", help="Use BLAKE3 for full-file hashing (fast) and confirm duplicates with SHA-256")
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
            max_workers=args.max_workers,
            network_friendly=args.network_friendly,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
            progressive=args.progressive,
            sample_bytes=args.sample_bytes,
            io_bytes_per_sec=args.io_bytes_per_sec,
            use_blake3=args.blake3,
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
        
        report = get_duplicate_report(
            Path(cfg.db.path),
            args.report_limit,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
        )
        
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
