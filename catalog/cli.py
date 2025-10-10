"""Unified command-line interface for Corpus Cataloger."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from . import dedupe, export, hash_all, scan
from .config import load_config

try:
    from . import gui
except Exception:  # pragma: no cover - GUI dependencies optional
    gui = None  # type: ignore


Handler = Callable[[argparse.Namespace], int]


def _scan_command(args: argparse.Namespace) -> int:
    sub_args: List[str] = ["--config", args.config]
    if args.max_workers is not None:
        sub_args.extend(["--max-workers", str(args.max_workers)])
    for root in args.root or []:
        sub_args.extend(["--root", root])
    scan.main(sub_args)
    return 0


def _hash_command(args: argparse.Namespace) -> int:
    sub_args: List[str] = ["--config", args.config]
    if args.force:
        sub_args.append("--force")
    if args.max_workers is not None:
        sub_args.extend(["--max-workers", str(args.max_workers)])
    for prefix in args.include_prefix or []:
        sub_args.extend(["--include-prefix", prefix])
    for prefix in args.exclude_prefix or []:
        sub_args.extend(["--exclude-prefix", prefix])
    if args.io_bytes_per_sec is not None:
        sub_args.extend(["--io-bytes-per-sec", str(args.io_bytes_per_sec)])
    if args.chunk_bytes is not None:
        sub_args.extend(["--chunk-bytes", str(args.chunk_bytes)])
    if args.mirror_to_sha256:
        sub_args.append("--mirror-to-sha256")
    hash_all.main(sub_args)
    return 0


def _dedupe_command(args: argparse.Namespace) -> int:
    sub_args: List[str] = ["--config", args.config]
    if args.max_workers is not None:
        sub_args.extend(["--max-workers", str(args.max_workers)])
    if args.network_friendly:
        sub_args.append("--network-friendly")
    for prefix in args.include_prefix or []:
        sub_args.extend(["--include-prefix", prefix])
    for prefix in args.exclude_prefix or []:
        sub_args.extend(["--exclude-prefix", prefix])
    if args.progressive:
        sub_args.append("--progressive")
    if args.sample_bytes is not None:
        sub_args.extend(["--sample-bytes", str(args.sample_bytes)])
    if args.io_bytes_per_sec is not None:
        sub_args.extend(["--io-bytes-per-sec", str(args.io_bytes_per_sec)])
    if args.blake3:
        sub_args.append("--blake3")
    if args.skip_quick_hash:
        sub_args.append("--skip-quick-hash")
    if args.skip_sha256:
        sub_args.append("--skip-sha256")
    if args.metadata_only:
        sub_args.append("--metadata-only")
    if args.metadata_prune:
        sub_args.append("--metadata-prune")
    if args.report:
        sub_args.append("--report")
    if args.report_only:
        sub_args.append("--report-only")
    if args.report_limit is not None:
        sub_args.extend(["--report-limit", str(args.report_limit)])
    if args.delete_duplicates:
        sub_args.append("--delete-duplicates")
    if args.dry_run:
        sub_args.append("--dry-run")
    if args.keep_newest:
        sub_args.append("--keep-newest")
    if args.no_confirm:
        sub_args.append("--no-confirm")
    dedupe.main(sub_args)
    return 0


def _export_command(args: argparse.Namespace) -> int:
    db_path: Optional[str] = args.db
    if not db_path:
        cfg = load_config(Path(args.config))
        db_path = cfg.db.path
    sub_args: List[str] = ["--db", db_path, "--out", args.out]
    export.main(sub_args)
    return 0


def _gui_command(args: argparse.Namespace) -> int:
    if gui is None:
        raise SystemExit("GUI dependencies (PySide6) are not installed.")
    if args.config:
        os.environ.setdefault("CATALOG_CONFIG_PATH", args.config)
    gui.main()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="catalog",
        description="Corpus Cataloger command-line interface",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")

    scan_parser = subparsers.add_parser("scan", help="Scan filesystem roots into the catalog")
    scan_parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    scan_parser.add_argument("--max-workers", type=int, help="Override scanner worker count")
    scan_parser.add_argument("--root", action="append", help="Additional root path to scan (may repeat)")
    scan_parser.set_defaults(handler=_scan_command)

    hash_parser = subparsers.add_parser("hash", help="Compute BLAKE3 hashes for catalog entries")
    hash_parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    hash_parser.add_argument("--force", action="store_true", help="Re-hash even if a digest already exists")
    hash_parser.add_argument("--max-workers", type=int, help="Override worker thread count")
    hash_parser.add_argument("--include-prefix", action="append", help="Only process files under this path (may repeat)")
    hash_parser.add_argument("--exclude-prefix", action="append", help="Skip files under this path (may repeat)")
    hash_parser.add_argument("--io-bytes-per-sec", type=int, help="Approximate global I/O rate limit (bytes/sec)")
    hash_parser.add_argument("--chunk-bytes", type=int, help="Chunk size for streaming reads (bytes)")
    hash_parser.add_argument("--mirror-to-sha256", action="store_true", help="Copy BLAKE3 digest into sha256 column")
    hash_parser.set_defaults(handler=_hash_command)

    dedupe_parser = subparsers.add_parser("dedupe", help="Detect and prune duplicate files")
    dedupe_parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    dedupe_parser.add_argument("--max-workers", type=int, help="Override worker thread count")
    dedupe_parser.add_argument("--network-friendly", action="store_true", help="Reduce concurrency and read sizes")
    dedupe_parser.add_argument("--include-prefix", action="append", help="Only process files under this path (may repeat)")
    dedupe_parser.add_argument("--exclude-prefix", action="append", help="Skip files under this path (may repeat)")
    dedupe_parser.add_argument("--progressive", action="store_true", help="Progressive sampling before full SHA")
    dedupe_parser.add_argument("--sample-bytes", type=int, help="Bytes to read for sampling")
    dedupe_parser.add_argument("--io-bytes-per-sec", type=int, help="Throttle file reads (bytes/sec)")
    dedupe_parser.add_argument("--blake3", action="store_true", help="Use BLAKE3 instead of SHA256")
    dedupe_parser.add_argument("--skip-quick-hash", action="store_true", help="Skip quick-hash stage")
    dedupe_parser.add_argument("--skip-sha256", action="store_true", help="Skip SHA256 stage")
    dedupe_parser.add_argument("--metadata-only", action="store_true", help="Use metadata-only duplicate detection")
    dedupe_parser.add_argument("--metadata-prune", action="store_true", help="Prune duplicate rows using metadata-only results")
    dedupe_parser.add_argument("--report", action="store_true", help="Print duplicate summary report")
    dedupe_parser.add_argument("--report-only", action="store_true", help="Only show report, skip detection")
    dedupe_parser.add_argument("--report-limit", type=int, help="Limit duplicate groups in report")
    dedupe_parser.add_argument("--delete-duplicates", action="store_true", help="Delete duplicate files from disk")
    dedupe_parser.add_argument("--dry-run", action="store_true", help="Preview duplicate deletions")
    dedupe_parser.add_argument("--keep-newest", action="store_true", help="Keep newest file instead of oldest")
    dedupe_parser.add_argument("--no-confirm", action="store_true", help="Skip confirmation prompt")
    dedupe_parser.set_defaults(handler=_dedupe_command)

    export_parser = subparsers.add_parser("export", help="Export catalog tables to Parquet")
    export_parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file (used when --db omitted)")
    export_parser.add_argument("--db", help="Explicit path to catalog SQLite database")
    export_parser.add_argument("--out", default="data/parquet", help="Destination folder for Parquet files")
    export_parser.set_defaults(handler=_export_command)

    gui_parser = subparsers.add_parser("gui", help="Launch the desktop GUI explorer")
    gui_parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    gui_parser.set_defaults(handler=_gui_command)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler: Optional[Handler] = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 1
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
