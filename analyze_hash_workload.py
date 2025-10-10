"""
Analyze how much data needs to be hashed (i.e., downloaded from network drive).
Shows total bytes that need to be read for duplicate detection.
"""
from catalog.db import connect
from pathlib import Path

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

def main():
    db_path = Path('data/projects.db')
    con = connect(db_path)
    cur = con.cursor()
    
    print("=" * 100)
    print("HASH WORKLOAD ANALYSIS")
    print("=" * 100)
    print()
    
    # Files needing hashing (sha256 IS NULL)
    cur.execute("""
        SELECT 
            COUNT(*) as file_count,
            SUM(size_bytes) as total_bytes,
            AVG(size_bytes) as avg_bytes,
            MIN(size_bytes) as min_bytes,
            MAX(size_bytes) as max_bytes
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND sha256 IS NULL
          AND path_abs LIKE 'S:%'
    """)
    
    row = cur.fetchone()
    files_needing_hash = row[0] or 0
    total_bytes = row[1] or 0
    avg_bytes = row[2] or 0
    min_bytes = row[3] or 0
    max_bytes = row[4] or 0
    
    print(f"FILES NEEDING FULL HASH:")
    print(f"  Total files: {files_needing_hash:,}")
    print(f"  Total size: {format_bytes(total_bytes)} ({total_bytes:,} bytes)")
    print(f"  Average file: {format_bytes(avg_bytes)}")
    print(f"  Smallest: {format_bytes(min_bytes)}")
    print(f"  Largest: {format_bytes(max_bytes)}")
    print()
    
    # Files in duplicate groups (size + ext matches)
    cur.execute("""
        WITH dup_groups AS (
            SELECT size_bytes, COALESCE(ext, '') AS ext
            FROM files
            WHERE state NOT IN ('error', 'missing')
              AND path_abs LIKE 'S:%'
              AND size_bytes >= 1024
            GROUP BY size_bytes, COALESCE(ext, '')
            HAVING COUNT(*) >= 2
        )
        SELECT 
            COUNT(*) as file_count,
            SUM(f.size_bytes) as total_bytes,
            AVG(f.size_bytes) as avg_bytes
        FROM files f
        INNER JOIN dup_groups dg 
          ON f.size_bytes = dg.size_bytes 
          AND COALESCE(f.ext, '') = dg.ext
        WHERE f.state NOT IN ('error', 'missing')
          AND f.path_abs LIKE 'S:%'
          AND f.sha256 IS NULL
    """)
    
    row = cur.fetchone()
    dup_candidates = row[0] or 0
    dup_bytes = row[1] or 0
    dup_avg = row[2] or 0
    
    print(f"POTENTIAL DUPLICATES (files in groups with 2+ same size+ext):")
    print(f"  Files to hash: {dup_candidates:,}")
    print(f"  Total size: {format_bytes(dup_bytes)} ({dup_bytes:,} bytes)")
    print(f"  Average file: {format_bytes(dup_avg)}")
    print()
    
    # Breakdown by size range
    print("BREAKDOWN BY FILE SIZE:")
    print("-" * 100)
    
    size_ranges = [
        ("< 1 MB", 0, 1024**2),
        ("1-10 MB", 1024**2, 10*1024**2),
        ("10-100 MB", 10*1024**2, 100*1024**2),
        ("100-500 MB", 100*1024**2, 500*1024**2),
        ("500 MB - 1 GB", 500*1024**2, 1024**3),
        ("> 1 GB", 1024**3, float('inf'))
    ]
    
    for label, min_size, max_size in size_ranges:
        if max_size == float('inf'):
            cur.execute("""
                SELECT COUNT(*), SUM(size_bytes)
                FROM files
                WHERE state NOT IN ('error', 'missing')
                  AND path_abs LIKE 'S:%'
                  AND sha256 IS NULL
                  AND size_bytes >= ?
            """, (min_size,))
        else:
            cur.execute("""
                SELECT COUNT(*), SUM(size_bytes)
                FROM files
                WHERE state NOT IN ('error', 'missing')
                  AND path_abs LIKE 'S:%'
                  AND sha256 IS NULL
                  AND size_bytes >= ?
                  AND size_bytes < ?
            """, (min_size, max_size))
        
        count, size = cur.fetchone()
        count = count or 0
        size = size or 0
        
        if count > 0:
            print(f"  {label:20s}: {count:6,} files = {format_bytes(size):>15s}")
    
    print()
    print("=" * 100)
    print("ESTIMATED PROCESSING TIME")
    print("=" * 100)
    print()
    
    # Estimate based on different scenarios
    network_speed_mbps = 100  # MB/s typical for gigabit network
    local_speed_mbps = 500    # MB/s typical for SSD
    
    network_time_sec = dup_bytes / (network_speed_mbps * 1024 * 1024)
    local_time_sec = dup_bytes / (local_speed_mbps * 1024 * 1024)
    
    print(f"Reading {format_bytes(dup_bytes)} of duplicate candidates:")
    print(f"  Network drive @ 100 MB/s: ~{network_time_sec/60:.1f} minutes ({network_time_sec/3600:.2f} hours)")
    print(f"  Local SSD @ 500 MB/s:     ~{local_time_sec/60:.1f} minutes ({local_time_sec/3600:.2f} hours)")
    print()
    
    # With 8 workers
    print(f"With 8 parallel workers:")
    print(f"  Network drive: ~{network_time_sec/60/8:.1f} minutes ({network_time_sec/3600/8:.2f} hours)")
    print(f"  Local SSD:     ~{local_time_sec/60/8:.1f} minutes ({local_time_sec/3600/8:.2f} hours)")
    print()
    
    print("NOTE: These are optimistic estimates. Real-world performance may be slower due to:")
    print("  - Network latency and overhead")
    print("  - File open/close operations")
    print("  - Database updates")
    print("  - System load")
    print()
    
    # Calculate what progressive mode would save
    cur.execute("""
        SELECT COUNT(*), SUM(size_bytes)
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND path_abs LIKE 'S:%'
          AND sha256 IS NULL
          AND size_bytes > 1048576
    """)
    
    large_count, large_bytes = cur.fetchone()
    large_count = large_count or 0
    large_bytes = large_bytes or 0
    
    # Progressive mode reads ~64KB per file instead of full file
    progressive_bytes = (large_count * 64 * 1024) + (dup_bytes - large_bytes)
    savings_bytes = dup_bytes - progressive_bytes
    savings_pct = (savings_bytes / dup_bytes * 100) if dup_bytes > 0 else 0
    
    print("=" * 100)
    print("PROGRESSIVE MODE BENEFIT")
    print("=" * 100)
    print()
    print(f"For files > 1 MB ({large_count:,} files, {format_bytes(large_bytes)}):")
    print(f"  Full hash reads:        {format_bytes(dup_bytes)}")
    print(f"  Progressive sampling:   {format_bytes(progressive_bytes)}")
    print(f"  Data savings:           {format_bytes(savings_bytes)} ({savings_pct:.1f}%)")
    print()
    print(f"Progressive mode estimated time @ 100 MB/s:")
    progressive_time = progressive_bytes / (network_speed_mbps * 1024 * 1024) / 8
    print(f"  ~{progressive_time:.1f} minutes ({progressive_time/60:.2f} hours)")
    print()
    
    con.close()

if __name__ == "__main__":
    main()
