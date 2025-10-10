"""
Prune metadata duplicates with sensible thresholds.
This will keep the OLDEST file in each duplicate group and remove the rest from the catalog.
FILES ON DISK ARE NOT TOUCHED - only catalog database entries are removed.
"""
import argparse
from catalog.config import load_config
from catalog.dedupe import detect_duplicates
from pathlib import Path
import sqlite3
from datetime import datetime

def parse_mtime(value: str) -> float:
    """Parse ISO timestamp to float for sorting."""
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return 0.0

def main():
    parser = argparse.ArgumentParser(
        description="Prune metadata duplicates from catalog database"
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
        "--dry-run",
        action="store_true",
        help="Show what would be removed without actually removing anything"
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Just show the duplicate report, don't prompt to prune"
    )
    
    args = parser.parse_args()
    
    cfg = load_config(Path(args.config))
    cfg.dedupe.min_file_size = args.min_size
    cfg.dedupe.min_duplicate_count = args.min_copies
    
    print("=" * 100)
    print("METADATA DUPLICATE DETECTION AND PRUNING")
    print("=" * 100)
    print(f"Config: {args.config}")
    print(f"Min file size: {args.min_size:,} bytes")
    print(f"Min copies: {args.min_copies}")
    if args.include_prefix:
        print(f"Include prefixes: {', '.join(args.include_prefix)}")
    print()
    
    # Run detection
    stats = detect_duplicates(
        cfg,
        metadata_only=True,
        include_prefixes=args.include_prefix or [],
    )
    
    print(f"\n{'=' * 100}")
    print(f"RESULTS: Found {stats['duplicate_groups']:,} duplicate groups")
    print(f"         Total {stats['duplicate_files']:,} duplicate files")
    print(f"{'=' * 100}\n")
    
    groups = stats.get('metadata_groups', [])
    
    if not groups:
        print("No duplicates found. Nothing to do.")
        return
    
    # Show top duplicates
    print("Top 20 duplicate groups by wasted space:")
    print("-" * 100)
    for i, g in enumerate(groups[:20], 1):
        size_mb = g["size_bytes"] / (1024**2)
        wasted_mb = size_mb * (len(g["members"]) - 1)
        print(f"{i}. {g['name']} ({g['ext'] or 'no ext'})")
        print(f"   {len(g['members'])} copies × {size_mb:.2f} MB = {wasted_mb:.2f} MB wasted")
    
    total_wasted_bytes = sum(
        g["size_bytes"] * (len(g["members"]) - 1)
        for g in groups
    )
    print(f"\nTotal wasted space: {total_wasted_bytes / (1024**3):.2f} GB")
    print()
    
    if args.report_only:
        print("Report-only mode. Exiting without pruning.")
        return
    
    # Prepare pruning list
    to_delete = []
    kept_summary = []
    
    for group in groups:
        members = group.get("members", [])
        if len(members) <= 1:
            continue
        
        # Sort by mtime (oldest first), then path, then file_id
        sorted_members = sorted(
            members,
            key=lambda m: (
                parse_mtime(m.get("mtime", "")),
                str(m.get("path", "")).lower(),
                int(m.get("file_id", 0)),
            ),
        )
        
        keeper = sorted_members[0]
        losers = sorted_members[1:]
        
        to_delete.extend(int(m["file_id"]) for m in losers if m.get("file_id") is not None)
        kept_summary.append({
            "name": group.get("name"),
            "kept": keeper,
            "removed": len(losers),
        })
    
    print(f"{'=' * 100}")
    print(f"PRUNING PLAN")
    print(f"{'=' * 100}")
    print(f"Will keep:   {len(groups):,} files (oldest in each group)")
    print(f"Will remove: {len(to_delete):,} catalog entries")
    print()
    print("Preview (first 10 groups):")
    for i, summary in enumerate(kept_summary[:10], 1):
        print(f"{i}. {summary['name']}")
        print(f"   KEEP: {summary['kept']['path']}")
        print(f"   REMOVE: {summary['removed']} other copies")
    
    if len(kept_summary) > 10:
        print(f"   ... and {len(kept_summary) - 10} more groups")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes made.")
        return
    
    # Confirm
    print("=" * 100)
    print("WARNING: This will permanently remove duplicate entries from the catalog database.")
    print("         FILES ON DISK WILL NOT BE TOUCHED.")
    print("=" * 100)
    response = input(f"\nProceed with removing {len(to_delete):,} duplicate entries? [y/N]: ")
    
    if response.lower() != 'y':
        print("Cancelled. No changes made.")
        return
    
    # Execute deletion
    print(f"\nRemoving {len(to_delete):,} duplicate entries...")
    
    con = sqlite3.connect(str(cfg.db.path))
    try:
        cur = con.cursor()
        CHUNK = 500
        removed_total = 0
        
        for idx in range(0, len(to_delete), CHUNK):
            chunk = to_delete[idx : idx + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            cur.execute(f"DELETE FROM files WHERE file_id IN ({placeholders})", chunk)
            removed_total += cur.rowcount
        
        con.commit()
        print(f"✓ Successfully removed {removed_total:,} duplicate entries from catalog.")
        print(f"✓ Freed approximately {total_wasted_bytes / (1024**3):.2f} GB of tracked duplicate space.")
    except Exception as e:
        print(f"✗ Error during deletion: {e}")
        con.rollback()
    finally:
        con.close()

if __name__ == "__main__":
    main()
