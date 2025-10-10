"""
Download/copy all cataloged files from network drive to local storage.
Maintains directory structure and provides progress tracking.
"""
import argparse
import shutil
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from catalog.db import connect

def format_bytes(size):
    """Format bytes into human readable string."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size/1024:.2f} KB"
    elif size < 1024**3:
        return f"{size/(1024**2):.2f} MB"
    else:
        return f"{size/(1024**3):.2f} GB"

def copy_file(src: Path, dest: Path, file_id: int) -> Tuple[int, bool, Optional[str]]:
    """Copy a single file, creating directories as needed."""
    try:
        # Create parent directory if needed
        dest.parent.mkdir(parents=True, exist_ok=True)
        
        # Skip if already exists and same size
        if dest.exists():
            if dest.stat().st_size == src.stat().st_size:
                return file_id, True, "already_exists"
        
        # Copy file
        shutil.copy2(str(src), str(dest))
        return file_id, True, None
    except FileNotFoundError:
        return file_id, False, "not_found"
    except PermissionError:
        return file_id, False, "permission_denied"
    except Exception as e:
        return file_id, False, str(e)

def main():
    parser = argparse.ArgumentParser(
        description="Download/copy all cataloged files to local storage"
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination directory (e.g., D:\\LocalCopy)"
    )
    parser.add_argument(
        "--source-prefix",
        default="S:\\",
        help="Source prefix to copy from (default: S:\\)"
    )
    parser.add_argument(
        "--replace-prefix",
        help="Replace source prefix with this in destination (e.g., D:\\S_Drive)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel copy workers (default: 4)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without actually copying"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip files that already exist at destination"
    )
    parser.add_argument(
        "--db",
        default="data/projects.db",
        help="Path to database (default: data/projects.db)"
    )
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    dest_root = Path(args.dest)
    
    print("=" * 100)
    print("FILE DOWNLOAD/COPY UTILITY")
    print("=" * 100)
    print()
    print(f"Database: {db_path}")
    print(f"Source prefix: {args.source_prefix}")
    print(f"Destination: {dest_root}")
    if args.replace_prefix:
        print(f"Prefix replacement: {args.source_prefix} → {args.replace_prefix}")
    print(f"Workers: {args.workers}")
    print(f"Dry run: {args.dry_run}")
    print(f"Skip existing: {args.skip_existing}")
    print()
    
    # Connect to database
    con = connect(db_path)
    cur = con.cursor()
    
    # Get files to copy
    print("Scanning database for files to copy...")
    cur.execute("""
        SELECT file_id, path_abs, size_bytes
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND path_abs LIKE ?
        ORDER BY size_bytes DESC
    """, (f"{args.source_prefix}%",))
    
    files = cur.fetchall()
    total_files = len(files)
    total_bytes = sum(row[2] or 0 for row in files)
    
    print(f"Found {total_files:,} files to copy")
    print(f"Total size: {format_bytes(total_bytes)} ({total_bytes:,} bytes)")
    print()
    
    # Estimate time
    copy_speed_mbps = 100  # MB/s typical for gigabit
    estimated_seconds = total_bytes / (copy_speed_mbps * 1024 * 1024)
    estimated_minutes = estimated_seconds / 60
    estimated_hours = estimated_minutes / 60
    
    print(f"Estimated copy time @ {copy_speed_mbps} MB/s:")
    if estimated_hours >= 1:
        print(f"  ~{estimated_hours:.1f} hours ({estimated_minutes:.0f} minutes)")
    else:
        print(f"  ~{estimated_minutes:.1f} minutes")
    print()
    
    # Ask for confirmation
    if not args.dry_run:
        print("=" * 100)
        print("WARNING: This will copy all files to local storage.")
        print(f"Destination: {dest_root}")
        print(f"Total size: {format_bytes(total_bytes)}")
        print("=" * 100)
        response = input(f"\nProceed with copying {total_files:,} files? [y/N]: ")
        
        if response.lower() != 'y':
            print("Cancelled.")
            return
        print()
    
    # Prepare copy tasks
    copy_tasks = []
    for file_id, path_abs, size_bytes in files:
        src = Path(path_abs)
        
        # Calculate destination path
        if args.replace_prefix:
            # Replace source prefix with replacement
            rel_path = path_abs[len(args.source_prefix):]
            dest = dest_root / args.replace_prefix / rel_path
        else:
            # Keep full path structure under dest_root
            # Remove drive letter (e.g., "S:\\" becomes just the path)
            rel_path = path_abs[len(args.source_prefix):]
            dest = dest_root / rel_path
        
        copy_tasks.append((file_id, src, dest, size_bytes))
    
    if args.dry_run:
        print("DRY RUN - showing first 20 copy operations:")
        print("-" * 100)
        for i, (file_id, src, dest, size_bytes) in enumerate(copy_tasks[:20], 1):
            print(f"{i}. {src}")
            print(f"   → {dest}")
            print(f"   Size: {format_bytes(size_bytes)}")
            print()
        
        if len(copy_tasks) > 20:
            print(f"... and {len(copy_tasks) - 20:,} more files")
        
        print(f"\nTotal: {total_files:,} files, {format_bytes(total_bytes)}")
        return
    
    # Perform copy
    print(f"Starting copy with {args.workers} workers...")
    print()
    
    copied = 0
    skipped = 0
    failed = 0
    bytes_copied = 0
    
    start_time = time.time()
    last_update = start_time
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(copy_file, src, dest, file_id): (src, dest, size_bytes)
            for file_id, src, dest, size_bytes in copy_tasks
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            src, dest, size_bytes = futures[future]
            file_id, success, error = future.result()
            
            if success:
                if error == "already_exists":
                    skipped += 1
                else:
                    copied += 1
                    bytes_copied += size_bytes
            else:
                failed += 1
                print(f"FAILED: {src}")
                print(f"  Error: {error}")
            
            total_processed = copied + skipped + failed
            
            # Update progress every second
            now = time.time()
            if now - last_update >= 1.0 or total_processed == total_files:
                elapsed = now - start_time
                rate = bytes_copied / elapsed if elapsed > 0 else 0
                rate_mbps = rate / (1024 * 1024)
                
                pct = (total_processed / total_files * 100) if total_files > 0 else 0
                remaining = (total_files - total_processed) / (total_processed / elapsed) if total_processed > 0 and elapsed > 0 else 0
                
                print(f"Progress: {total_processed:,}/{total_files:,} ({pct:.1f}%) | "
                      f"Copied: {copied:,} | Skipped: {skipped:,} | Failed: {failed:,} | "
                      f"Speed: {rate_mbps:.1f} MB/s | "
                      f"ETA: {remaining/60:.1f}m")
                
                last_update = now
    
    elapsed = time.time() - start_time
    avg_speed = bytes_copied / elapsed if elapsed > 0 else 0
    
    print()
    print("=" * 100)
    print("COPY COMPLETE")
    print("=" * 100)
    print(f"Total files: {total_files:,}")
    print(f"Copied: {copied:,} files ({format_bytes(bytes_copied)})")
    print(f"Skipped: {skipped:,} files (already existed)")
    print(f"Failed: {failed:,} files")
    print(f"Time: {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")
    print(f"Average speed: {avg_speed/(1024*1024):.1f} MB/s")
    print()
    
    # Update database with errors if any
    if failed > 0:
        print(f"Note: {failed} files failed to copy. Check output above for details.")
    
    con.close()

if __name__ == "__main__":
    main()
