from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .config import CatalogConfig, load_config
from .db import connect, migrate
from .util import blake3_file

try:
    import blake3  # noqa: F401
    HAS_BLAKE3 = True
except Exception:
    HAS_BLAKE3 = False

ProgressCallback = Callable[[str, int, int, str], None]
LogCallback = Callable[[str], None]


@dataclass
class HashStats:
    total_candidates: int = 0
    hashed: int = 0
    skipped_existing: int = 0
    missing: int = 0
    errors: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "total_candidates": self.total_candidates,
            "hashed": self.hashed,
            "skipped_existing": self.skipped_existing,
            "missing": self.missing,
            "errors": self.errors,
        }


class ByteRateLimiter:
    def __init__(self, rate_bps: int, burst: Optional[int] = None) -> None:
        from threading import Lock

        self.rate = max(1, int(rate_bps))
        self.capacity = int(burst or self.rate)
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self._lock = Lock()

    def acquire(self, size: int) -> None:
        if size <= 0:
            return
        while True:
            now = time.monotonic()
            with self._lock:
                elapsed = now - self.last
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.last = now
                if self.tokens >= size:
                    self.tokens -= size
                    return
                required = size - self.tokens
            to_sleep = max(0.001, required / float(self.rate))
            time.sleep(to_sleep)


def path_filter_sql(include: Sequence[str], exclude: Sequence[str], alias: str = "f") -> Tuple[str, List[str]]:
    clauses: List[str] = []
    params: List[str] = []
    if include:
        ors = []
        for prefix in include:
            ors.append(f"{alias}.path_abs LIKE ?")
            params.append(prefix.rstrip('\\/') + '%')
        clauses.append('(' + ' OR '.join(ors) + ')')
    if exclude:
        ors = []
        for prefix in exclude:
            ors.append(f"{alias}.path_abs LIKE ?")
            params.append(prefix.rstrip('\\/') + '%')
        clauses.append('NOT (' + ' OR '.join(ors) + ')')
    if clauses:
        return ' AND ' + ' AND '.join(clauses) + ' ', params
    return '', params


def _emit(cb: Optional[Callable[..., None]], *args, **kwargs) -> None:
    if not cb:
        return
    try:
        cb(*args, **kwargs)
    except Exception:
        pass


def hash_all_blake3(
    cfg: CatalogConfig,
    force: bool = False,
    max_workers: Optional[int] = None,
    include_prefixes: Optional[Sequence[str]] = None,
    exclude_prefixes: Optional[Sequence[str]] = None,
    io_bytes_per_sec: Optional[int] = None,
    chunk_bytes: Optional[int] = None,
    mirror_to_sha256: bool = False,
    progress_cb: Optional[ProgressCallback] = None,
    log_cb: Optional[LogCallback] = None,
) -> HashStats:
    if not HAS_BLAKE3:
        raise RuntimeError("The 'blake3' package is not available. Install it with 'pip install blake3'.")

    include_prefixes = tuple(include_prefixes or [])
    exclude_prefixes = tuple(exclude_prefixes or [])
    workers = max_workers or cfg.dedupe.max_workers or cfg.scanner.max_workers
    chunk_size = chunk_bytes or cfg.dedupe.sha_chunk_bytes

    limiter: Optional[ByteRateLimiter] = None
    if io_bytes_per_sec and io_bytes_per_sec > 0:
        limiter = ByteRateLimiter(io_bytes_per_sec)

    def emit_progress(stage: str, current: int, total: int, message: str) -> None:
        _emit(progress_cb, stage, current, total, message)

    def emit_log(message: str) -> None:
        print(message)
        _emit(log_cb, message)

    emit_progress("start", 0, 0, "Starting BLAKE3 hashing for all files")
    emit_log(f"[BLAKE3] Hashing all files with up to {workers} workers | chunk={chunk_size:,} bytes")

    db_path = Path(cfg.db.path)
    if not db_path.exists():
        raise FileNotFoundError(f"Catalog database not found at {db_path}")

    con = connect(db_path)
    migrate(con)
    stats = HashStats()

    try:
        cur = con.cursor()
        filt_sql, filt_params = path_filter_sql(include_prefixes, exclude_prefixes)
        where_clauses = ["f.state NOT IN ('error', 'missing')"]
        params: List[object] = list(filt_params)
        if not force:
            where_clauses.append("(f.blake3 IS NULL OR f.blake3 = '')")
        where_clause = " AND ".join(where_clauses)

        query = (
            "SELECT f.file_id, f.path_abs, f.size_bytes, f.blake3 FROM files f "
            "WHERE " + where_clause + filt_sql + " ORDER BY f.file_id"
        )
        cur.execute(query, params)
        rows = cur.fetchall()
        stats.total_candidates = len(rows)
        if stats.total_candidates == 0:
            emit_log("[BLAKE3] No files require hashing; nothing to do")
            emit_progress("done", 0, 0, "No files required BLAKE3 hashes")
            return stats

        emit_log(f"[BLAKE3] {stats.total_candidates:,} files queued for hashing")
        emit_progress("hash", 0, stats.total_candidates, "Preparing workers")

        batch_success: List[Tuple[str, int]] = []
        batch_success_sha: List[Tuple[str, int]] = []
        batch_missing: List[Tuple[str, str, str, int]] = []  # (code, message, path, file_id)
        batch_error: List[Tuple[str, str, str, int]] = []    # (code, message, path, file_id)
        BATCH = 200
        processed = 0
        start = time.time()
        last_log = start

        def update_success() -> None:
            nonlocal batch_success, batch_success_sha
            if not batch_success and not batch_success_sha:
                return
            if batch_success:
                cur.executemany(
                    "UPDATE files SET blake3=?, state=CASE WHEN state IN ('error','missing') THEN state ELSE 'sha_verified' END WHERE file_id=?",
                    batch_success,
                )
                batch_success = []
            if mirror_to_sha256 and batch_success_sha:
                cur.executemany(
                    "UPDATE files SET sha256=? WHERE file_id=?",
                    batch_success_sha,
                )
                batch_success_sha = []
            con.commit()

        def update_missing() -> None:
            nonlocal batch_missing
            if not batch_missing:
                return
            cur.executemany(
                "UPDATE files SET state='missing', error_code=?, error_msg=?, blake3=NULL WHERE file_id=?",
                [
                    (code, f"{msg} ({path})" if msg else f"Missing file: {path}", fid)
                    for code, msg, path, fid in batch_missing
                ],
            )
            con.commit()
            batch_missing = []

        def update_errors() -> None:
            nonlocal batch_error
            if not batch_error:
                return
            cur.executemany(
                "UPDATE files SET state='error', error_code=?, error_msg=?, blake3=NULL WHERE file_id=?",
                [
                    (code, f"{msg} ({path})" if msg else f"Hash failure: {path}", fid)
                    for code, msg, path, fid in batch_error
                ],
            )
            con.commit()
            batch_error = []

        def flush_all() -> None:
            update_success()
            update_missing()
            update_errors()

        def compute(row: Tuple[int, str, int, Optional[str]]) -> Tuple[int, str, Optional[str]]:
            file_id, path_abs, size_bytes, existing = row
            if existing and not force:
                return (file_id, 'skip', existing)
            path = Path(path_abs)
            if not path.exists():
                return (file_id, 'missing', "File not found on disk")
            try:
                if limiter:
                    digest = _blake3_with_limiter(path, chunk_size, limiter)
                else:
                    digest = blake3_file(path, chunk_size)
                return (file_id, 'ok', digest)
            except Exception as exc:
                return (file_id, 'error', str(exc))

        def _blake3_with_limiter(path: Path, chunk_size: int, limiter_obj: ByteRateLimiter) -> str:
            try:
                import blake3  # type: ignore
            except Exception as exc:
                raise RuntimeError("The 'blake3' package is required to hash files") from exc
            hasher = blake3.blake3()
            with path.open('rb', buffering=1024 * 64) as fh:
                while True:
                    chunk = fh.read(chunk_size)
                    if not chunk:
                        break
                    limiter_obj.acquire(len(chunk))
                    hasher.update(chunk)
            return hasher.hexdigest()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(compute, row): row for row in rows}
            for fut in as_completed(futures):
                file_id, status, payload = fut.result()
                processed += 1

                if status == 'ok' and payload:
                    batch_success.append((payload, file_id))
                    if mirror_to_sha256:
                        batch_success_sha.append((payload, file_id))
                    stats.hashed += 1
                elif status == 'skip':
                    stats.skipped_existing += 1
                elif status == 'missing':
                    path_abs = futures[fut][1]
                    batch_missing.append(("not_found", payload or "", path_abs, file_id))
                    emit_log(f"[WARN] Missing file: {path_abs}")
                    stats.missing += 1
                else:
                    path_abs = futures[fut][1]
                    batch_error.append(("hash_failed", payload or "", path_abs, file_id))
                    emit_log(f"[ERROR] Failed to hash {path_abs}: {payload}")
                    stats.errors += 1

                if len(batch_success) >= BATCH or len(batch_success_sha) >= BATCH:
                    update_success()
                if len(batch_missing) >= BATCH:
                    update_missing()
                if len(batch_error) >= BATCH:
                    update_errors()

                if processed % 100 == 0 or processed == stats.total_candidates:
                    elapsed = time.time() - start
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (stats.total_candidates - processed) / rate if rate > 0 else 0
                    emit_progress(
                        "hash",
                        processed,
                        stats.total_candidates,
                        f"Processed {processed:,}/{stats.total_candidates:,} files ({rate:.1f}/s, ETA {eta/60:.1f}m)",
                    )
                    now = time.time()
                    if now - last_log >= 30 or processed == stats.total_candidates:
                        emit_log(
                            f"[BLAKE3] {processed:,}/{stats.total_candidates:,} | hashed={stats.hashed:,} "
                            f"skipped={stats.skipped_existing:,} missing={stats.missing:,} errors={stats.errors:,} | {rate:.1f} files/sec"
                        )
                        last_log = now

        flush_all()
        emit_progress("done", stats.hashed, stats.total_candidates, "Completed BLAKE3 hashing")
        emit_log("[BLAKE3] Hashing complete")
        return stats
    finally:
        con.close()


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Compute BLAKE3 hashes for every catalog file")
    parser.add_argument("--config", default="config/catalog.yaml", help="Path to catalog config file")
    parser.add_argument("--force", action="store_true", help="Re-hash files even if a BLAKE3 digest already exists")
    parser.add_argument("--max-workers", type=int, help="Override worker thread count")
    parser.add_argument("--include-prefix", action="append", default=None, help="Only process files whose absolute paths start with this prefix (can repeat)")
    parser.add_argument("--exclude-prefix", action="append", default=None, help="Skip files whose absolute paths start with this prefix (can repeat)")
    parser.add_argument("--io-bytes-per-sec", type=int, help="Approximate global I/O rate limit in bytes per second")
    parser.add_argument("--chunk-bytes", type=int, help="Chunk size (bytes) for streaming reads; defaults to config.dedupe.sha_chunk_bytes")
    parser.add_argument("--mirror-to-sha256", action="store_true", help="Copy the BLAKE3 digest into the sha256 column for compatibility")
    args = parser.parse_args(list(argv) if argv is not None else None)

    cfg = load_config(Path(args.config))
    stats = hash_all_blake3(
        cfg,
        force=args.force,
        max_workers=args.max_workers,
        include_prefixes=args.include_prefix or [],
        exclude_prefixes=args.exclude_prefix or [],
        io_bytes_per_sec=args.io_bytes_per_sec,
        chunk_bytes=args.chunk_bytes,
        mirror_to_sha256=args.mirror_to_sha256,
    )

    print("\n" + "=" * 80)
    print("BLAKE3 HASH SUMMARY")
    print("=" * 80)
    print(f"Total candidates:    {stats.total_candidates:>10,}")
    print(f"Hashed:              {stats.hashed:>10,}")
    print(f"Skipped existing:    {stats.skipped_existing:>10,}")
    print(f"Missing files:       {stats.missing:>10,}")
    print(f"Errors:              {stats.errors:>10,}")
    print("=" * 80)


if __name__ == "__main__":
    main()
