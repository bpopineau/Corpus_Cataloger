"""CLI command for duplicate detection and pruning."""
from __future__ import annotations

import argparse
from argparse import _SubParsersAction
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from ..config import load_config
from ..db import connect
from ..dedupe import detect_duplicates, get_duplicate_report, prune_hash_duplicates


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/catalog.yaml", help="Path to config file")
    parser.add_argument("--max-workers", type=int, help="Number of worker threads")
    parser.add_argument("--network-friendly", action="store_true", help="Reduce network I/O (lower concurrency, smaller quick-hash window)")
    parser.add_argument("--include-prefix", action="append", help="Only process files with absolute paths starting with this prefix (can repeat)")
    parser.add_argument("--exclude-prefix", action="append", help="Skip files with absolute paths starting with this prefix (can repeat)")
    parser.add_argument("--progressive", action="store_true", help="Progressive staged sampling (head/tail) before full SHA; persists h1/h2 in DB")
    parser.add_argument("--sample-bytes", type=int, help="Bytes to read for head/tail sampling (default: min(quick_hash_bytes, 64KiB))")
    parser.add_argument("--io-bytes-per-sec", type=int, help="Throttle file reading to this many bytes/sec (approximate)")
    parser.add_argument("--blake3", action="store_true", help="Use BLAKE3 for full-file hashing (fast) and confirm duplicates with SHA-256")
    parser.add_argument("--skip-quick-hash", action="store_true", help="Skip quick hash stage")
    parser.add_argument("--skip-sha256", action="store_true", help="Skip SHA256 stage")
    parser.add_argument("--metadata-only", action="store_true", help="Use metadata-only duplicate detection (size+name) without hashing")
    parser.add_argument("--metadata-prune", action="store_true", help="After metadata-only detection, remove duplicate rows from the catalog database (keeps the oldest entry)")
    parser.add_argument("--report", action="store_true", help="Show duplicate report after detection")
    parser.add_argument("--report-only", action="store_true", help="Only show report, skip detection")
    parser.add_argument("--report-limit", type=int, default=100, help="Limit number of duplicate groups in report")
    parser.add_argument("--delete-duplicates", action="store_true", help="Delete duplicate files on disk and prune their catalog rows after detection")
    parser.add_argument("--dry-run", action="store_true", help="Preview duplicate deletions without touching disk or database")
    parser.add_argument("--keep-newest", action="store_true", help="Keep the newest file in each hash group (default keeps oldest)")
    parser.add_argument("--no-confirm", action="store_true", help="Skip confirmation prompt before deleting duplicates")


def add_parser(subparsers: _SubParsersAction[argparse.ArgumentParser]) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "dedupe",
        help="Detect and prune duplicate files",
        description="Identify duplicate files via hashing or metadata-only strategies and optionally prune them.",
    )
    _configure_parser(parser)
    parser.set_defaults(handler=run_from_args)
    return parser


def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog or "catalog dedupe", description="Detect duplicate catalog entries and optionally prune them")
    _configure_parser(parser)
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    cfg = load_config(Path(args.config))

    if args.metadata_prune:
        args.metadata_only = True
        args.skip_quick_hash = True
        args.skip_sha256 = True

    if args.metadata_only and args.report_only:
        print("Metadata-only mode ignores --report-only; running detection instead.")

    stats: Optional[Dict[str, Any]] = None

    if not args.report_only or args.metadata_only:
        enable_quick_hash = False if args.metadata_only else not args.skip_quick_hash
        enable_sha256 = False if args.metadata_only else not args.skip_sha256

        stats = detect_duplicates(
            cfg,
            enable_quick_hash=enable_quick_hash,
            enable_sha256=enable_sha256,
            max_workers=args.max_workers,
            network_friendly=args.network_friendly,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
            progressive=args.progressive,
            sample_bytes=args.sample_bytes,
            io_bytes_per_sec=args.io_bytes_per_sec,
            use_blake3=args.blake3,
            metadata_only=args.metadata_only,
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
        if args.metadata_only and stats.get("metadata_groups"):
            print("\nTop metadata duplicate groups (size, name, count):")
            for group in stats["metadata_groups"][: min(10, len(stats["metadata_groups"]))]:
                size_mb = group["size_bytes"] / (1024**2)
                print(f"  - {group['name']} ({group['ext'] or ''}) | {len(group['members'])} copies | {size_mb:.2f} MB each")
                for member in group["members"][:3]:
                    print(f"      • {member['path']}")
                if len(group["members"]) > 3:
                    print(f"      • ... {len(group['members']) - 3} more")

    if args.metadata_prune:
        if not stats or not stats.get("metadata_groups"):
            print("\nNo metadata duplicate groups found; nothing to prune.")
        else:
            print("\nPruning duplicate catalog rows based on metadata groups...")
            to_delete: List[int] = []
            kept_summary: List[Dict[str, Any]] = []
            pruned_groups = 0
            pruned_files = 0

            def _parse_mtime(value: str) -> float:
                if not value:
                    return 0.0
                try:
                    return datetime.fromisoformat(value).timestamp()
                except Exception:
                    return 0.0

            for group in stats.get("metadata_groups", []):
                members = group.get("members", [])
                if len(members) <= 1:
                    continue
                sorted_members = sorted(
                    members,
                    key=lambda m: (
                        _parse_mtime(m.get("mtime", "")),
                        str(m.get("path", "")).lower(),
                        int(m.get("file_id", 0)),
                    ),
                )
                keeper = sorted_members[0]
                losers = sorted_members[1:]
                pruned_groups += 1
                pruned_files += len(losers)
                to_delete.extend(int(m["file_id"]) for m in losers if m.get("file_id") is not None)
                kept_summary.append(
                    {
                        "name": group.get("name"),
                        "ext": group.get("ext"),
                        "size_bytes": group.get("size_bytes"),
                        "kept": keeper,
                        "removed": losers,
                    }
                )

            if not to_delete:
                print("No removable duplicates identified.")
            else:
                con = connect(Path(cfg.db.path))
                try:
                    cur = con.cursor()
                    CHUNK = 500
                    for idx in range(0, len(to_delete), CHUNK):
                        chunk = to_delete[idx : idx + CHUNK]
                        placeholders = ",".join("?" for _ in chunk)
                        cur.execute(f"DELETE FROM files WHERE file_id IN ({placeholders})", chunk)
                    con.commit()
                finally:
                    con.close()

                print(
                    f"Removed {pruned_files:,} catalog rows across {pruned_groups:,} metadata duplicate groups (kept the oldest entry in each)."
                )
                preview_summary = kept_summary[:5]
                for entry in preview_summary:
                    size_mb = (entry.get("size_bytes") or 0) / (1024**2)
                    print(
                        f"  • Kept {entry['kept'].get('path')} ({size_mb:.2f} MB); removed {len(entry['removed'])} siblings."
                    )
                if len(kept_summary) > len(preview_summary):
                    print(f"  • ... {len(kept_summary) - len(preview_summary)} more groups pruned")

    if args.delete_duplicates:
        if args.metadata_only:
            print("\nCannot delete duplicates while running in metadata-only mode. Rerun without --metadata-only.")
            return 0
        if stats is None:
            raise SystemExit("Duplicate detection must run before --delete-duplicates.")
        keep_strategy = "newest" if args.keep_newest else "oldest"
        preview: Dict[str, Any] = prune_hash_duplicates(
            cfg,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
            dry_run=True,
            keep_strategy=keep_strategy,
        )

        potential_gb = preview["potential_bytes_reclaimed"] / float(1024**3)
        print("\n" + "=" * 70)
        print("HASH DUPLICATE PRUNE (PREVIEW)")
        print("=" * 70)
        print(f"Duplicate hash groups:   {preview['hash_groups']:>10,}")
        print(f"Groups with removals:    {preview['groups_modified']:>10,}")
        print(f"Duplicate files flagged: {preview['files_considered']:>10,}")
        print(f"Potential space freed:   {potential_gb:>10.2f} GB")

        if preview["files_considered"] == 0:
            print("No duplicate files eligible for removal.")
            return 0

        sample_groups = preview.get("groups", [])[: min(5, len(preview.get("groups", [])))]
        if sample_groups:
            print("\nExamples:")
            for group in sample_groups:
                keeper = group.get("keeper", {})
                print(f"  • Keep: {keeper.get('path')}")
                duplicates = group.get("duplicates", [])
                for dup in duplicates[:3]:
                    print(f"      - Remove: {dup.get('path')}")
                if len(duplicates) > 3:
                    print(f"      - ... {len(duplicates) - 3} more duplicates")

        if args.dry_run:
            print("\nDry run requested; no files were removed.")
            return 0

        proceed = True
        if not args.no_confirm:
            prompt = f"\nProceed with deleting {preview['files_considered']:,} duplicate files? [y/N]: "
            proceed = input(prompt).strip().lower() == "y"
        if not proceed:
            print("Duplicate deletion cancelled. Rerun with --dry-run to preview or with --no-confirm to skip prompts.")
            return 0

        print("\nExecuting duplicate removal...")
        result: Dict[str, Any] = prune_hash_duplicates(
            cfg,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
            dry_run=False,
            keep_strategy=keep_strategy,
        )
        reclaimed_gb = result["bytes_reclaimed"] / float(1024**3)
        print("\n" + "=" * 70)
        print("HASH DUPLICATE PRUNE (RESULT)")
        print("=" * 70)
        print(f"Groups processed:        {result['hash_groups']:>10,}")
        print(f"Groups modified:         {result['groups_modified']:>10,}")
        print(f"Files removed:           {result['files_removed']:>10,}")
        print(f"Catalog rows removed:    {result['db_rows_removed']:>10,}")
        print(f"Space reclaimed:         {reclaimed_gb:>10.2f} GB")
        if result["errors"]:
            print("\nErrors encountered (showing up to 5):")
            for err in result["errors"][:5]:
                print(f"  - {err}")
            if len(result["errors"]) > 5:
                print(f"  - ... {len(result['errors']) - 5} more issues")

    if args.metadata_only and (args.report or args.report_only):
        print("\nMetadata-only mode does not compute SHA256 hashes; skipping hash-based duplicate report.")
        if stats and stats.get("metadata_groups"):
            print("Top metadata duplicate groups already listed above.")
        return 0

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
            print("    Files:")
            for path_info in group["paths"]:
                print(f"      - {path_info['path']}")
                print(f"        Modified: {path_info['mtime']}")

    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run_from_args(args)


__all__ = ["add_parser", "build_parser", "run_cli", "run_from_args"]
