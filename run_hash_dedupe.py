"""
Run hash-based duplicate detection with configurable thresholds.
This computes quick hashes and SHA256 for cryptographic duplicate verification.
"""
import argparse
from catalog.config import load_config
from catalog.dedupe import detect_duplicates, get_duplicate_report, prune_hash_duplicates
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(
        description="Run hash-based duplicate detection"
    )
    parser.add_argument(
        "--config",
        default="config/catalog.yaml",
        help="Path to config file (default: config/catalog.yaml)"
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=1024,
        help="Minimum file size in bytes (default: 1024 = 1KB)"
    )
    parser.add_argument(
        "--min-copies",
        type=int,
        default=2,
        help="Minimum number of copies to be considered a duplicate (default: 2)"
    )
    parser.add_argument(
        "--include-prefix",
        action="append",
        help="Only process files with paths starting with this prefix (can repeat)"
    )
    parser.add_argument(
        "--exclude-prefix",
        action="append",
        help="Skip files with paths starting with this prefix (can repeat)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Number of worker threads (default: use config value)"
    )
    parser.add_argument(
        "--network-friendly",
        action="store_true",
        help="Reduce network I/O (lower concurrency, smaller chunks)"
    )
    parser.add_argument(
        "--progressive",
        action="store_true",
        help="Progressive staged sampling (head/tail) before full SHA"
    )
    parser.add_argument(
        "--blake3",
        action="store_true",
        help="Use BLAKE3 for hashing (~10x faster than SHA256)"
    )
    parser.add_argument(
        "--report-limit",
        type=int,
        default=20,
        help="Number of duplicate groups to show in report (default: 20)"
    )
    parser.add_argument(
        "--delete-duplicates",
        action="store_true",
        help="Delete duplicate files on disk and prune catalog rows after detection"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without touching disk or database"
    )
    parser.add_argument(
        "--keep-newest",
        action="store_true",
        help="Keep the newest file in each duplicate group (default keeps oldest)"
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompt before deleting duplicates"
    )
    
    args = parser.parse_args()
    
    cfg = load_config(Path(args.config))
    cfg.dedupe.min_file_size = args.min_size
    cfg.dedupe.min_duplicate_count = args.min_copies
    
    print("=" * 100)
    print("HASH-BASED DUPLICATE DETECTION")
    print("=" * 100)
    print(f"Config: {args.config}")
    print(f"Min file size: {args.min_size:,} bytes")
    print(f"Min copies: {args.min_copies}")
    print(f"Max workers: {args.max_workers or cfg.dedupe.max_workers}")
    print(f"Progressive mode: {args.progressive}")
    print(f"BLAKE3 mode: {args.blake3} (much faster than SHA256)")
    print(f"Network friendly: {args.network_friendly}")
    if args.include_prefix:
        print(f"Include prefixes: {', '.join(args.include_prefix)}")
    print()
    
    # Run detection
    stats = detect_duplicates(
        cfg,
        enable_quick_hash=True,
        enable_sha256=True,
        max_workers=args.max_workers,
        network_friendly=args.network_friendly,
        include_prefixes=args.include_prefix or [],
        exclude_prefixes=args.exclude_prefix or [],
        progressive=args.progressive,
        use_blake3=args.blake3,
    )
    
    print(f"\n{'=' * 100}")
    print("DUPLICATE DETECTION SUMMARY")
    print(f"{'=' * 100}")
    print(f"Files processed:       {stats['files_processed']:>10,}")
    print(f"Quick hashes:          {stats['quick_hash_count']:>10,}")
    print(f"SHA256 hashes:         {stats['sha256_count']:>10,}")
    print(f"Files missing:         {stats['files_missing']:>10,}")
    print(f"Files with errors:     {stats['files_error']:>10,}")
    print(f"Duplicate groups:      {stats['duplicate_groups']:>10,}")
    print(f"Total duplicate files: {stats['duplicate_files']:>10,}")
    print(f"{'=' * 100}\n")
    
    if stats['duplicate_groups'] > 0:
        print(f"\n{'=' * 100}")
        print(f"TOP {args.report_limit} DUPLICATE GROUPS (by wasted space)")
        print(f"{'=' * 100}\n")
        
        report = get_duplicate_report(
            Path(cfg.db.path),
            args.report_limit,
            include_prefixes=args.include_prefix or [],
            exclude_prefixes=args.exclude_prefix or [],
        )
        
        for i, group in enumerate(report, 1):
            wasted_mb = group["total_wasted"] / (1024**2)
            size_mb = group["size_bytes"] / (1024**2)
            print(f"#{i} - {group['count']} copies × {size_mb:.2f} MB = {wasted_mb:.2f} MB wasted")
            print(f"    SHA256: {group['sha256'][:16]}...")
            print(f"    Files:")
            for path_info in group["paths"]:
                print(f"      - {path_info['path']}")
                if path_info.get('mtime'):
                    print(f"        Modified: {path_info['mtime']}")
            print()

        if args.delete_duplicates:
            keep_strategy = "newest" if args.keep_newest else "oldest"
            preview = prune_hash_duplicates(
                cfg,
                include_prefixes=args.include_prefix or [],
                exclude_prefixes=args.exclude_prefix or [],
                dry_run=True,
                keep_strategy=keep_strategy,
            )

            potential_gb = preview["potential_bytes_reclaimed"] / float(1024**3)
            print("\n" + "=" * 100)
            print("HASH DUPLICATE PRUNE (PREVIEW)")
            print("=" * 100)
            print(f"Duplicate hash groups:   {preview['hash_groups']:>10,}")
            print(f"Groups with removals:    {preview['groups_modified']:>10,}")
            print(f"Duplicate files flagged: {preview['files_considered']:>10,}")
            print(f"Potential space freed:   {potential_gb:>10.2f} GB")

            if preview["files_considered"] == 0:
                print("No duplicate files eligible for removal.")
                return

            if args.dry_run:
                print("Dry run requested; no files were removed.")
                return

            proceed = True
            if not args.no_confirm:
                prompt = f"Proceed with deleting {preview['files_considered']:,} duplicate files? [y/N]: "
                proceed = input(prompt).strip().lower() == "y"
            if not proceed:
                print("Duplicate deletion cancelled. Rerun with --dry-run to preview or with --no-confirm to skip prompts.")
                return

            result = prune_hash_duplicates(
                cfg,
                include_prefixes=args.include_prefix or [],
                exclude_prefixes=args.exclude_prefix or [],
                dry_run=False,
                keep_strategy=keep_strategy,
            )
            reclaimed_gb = result["bytes_reclaimed"] / float(1024**3)
            print("\n" + "=" * 100)
            print("HASH DUPLICATE PRUNE (RESULT)")
            print("=" * 100)
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

if __name__ == "__main__":
    main()
