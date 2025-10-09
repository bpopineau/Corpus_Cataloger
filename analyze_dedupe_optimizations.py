#!/usr/bin/env python3
"""
Analyze potential optimizations to reduce the number of files being hashed.
"""
from pathlib import Path
from catalog.config import load_config
from catalog.db import connect

cfg = load_config(Path("config/catalog.yaml"))
con = connect(Path(cfg.db.path))
cur = con.cursor()

small_file_threshold = cfg.dedupe.small_file_threshold

print("=" * 80)
print("DEDUPE OPTIMIZATION ANALYSIS")
print("=" * 80)

# Current baseline
cur.execute("""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    )
    SELECT COUNT(*)
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.quick_hash IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= ?
""", (small_file_threshold,))

baseline = cur.fetchone()[0]
print(f"\n📊 BASELINE: {baseline:,} files need quick hashing")
print("=" * 80)

# Strategy 1: Increase minimum file size
print("\n💡 STRATEGY 1: Increase minimum file size threshold")
print("-" * 80)

for min_size_kb in [256, 512, 1024, 2048]:
    min_size_bytes = min_size_kb * 1024
    cur.execute("""
        WITH dup_candidates AS (
            SELECT size_bytes, COALESCE(ext, '') AS ext
            FROM files
            WHERE state NOT IN ('error', 'missing')
            GROUP BY size_bytes, COALESCE(ext, '')
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*)
        FROM files f
        INNER JOIN dup_candidates dc
          ON f.size_bytes = dc.size_bytes
         AND COALESCE(f.ext, '') = dc.ext
        WHERE f.quick_hash IS NULL
          AND f.state NOT IN ('error', 'missing')
          AND f.size_bytes >= ?
    """, (min_size_bytes,))
    
    count = cur.fetchone()[0]
    reduction = baseline - count
    pct = (reduction / baseline * 100) if baseline > 0 else 0
    print(f"  Min size {min_size_kb:>4} KB: {count:>7,} files  (saves {reduction:>7,} files, -{pct:.1f}%)")

# Strategy 2: Require 3+ files in group instead of 2+
print("\n💡 STRATEGY 2: Only hash groups with 3+ files (not just 2+)")
print("-" * 80)

for min_group_size in [3, 4, 5, 10]:
    cur.execute("""
        WITH dup_candidates AS (
            SELECT size_bytes, COALESCE(ext, '') AS ext
            FROM files
            WHERE state NOT IN ('error', 'missing')
            GROUP BY size_bytes, COALESCE(ext, '')
            HAVING COUNT(*) >= ?
        )
        SELECT COUNT(*)
        FROM files f
        INNER JOIN dup_candidates dc
          ON f.size_bytes = dc.size_bytes
         AND COALESCE(f.ext, '') = dc.ext
        WHERE f.quick_hash IS NULL
          AND f.state NOT IN ('error', 'missing')
          AND f.size_bytes >= ?
    """, (min_group_size, small_file_threshold))
    
    count = cur.fetchone()[0]
    reduction = baseline - count
    pct = (reduction / baseline * 100) if baseline > 0 else 0
    print(f"  Min group size {min_group_size:>2}: {count:>7,} files  (saves {reduction:>7,} files, -{pct:.1f}%)")

# Strategy 3: Focus on high-value extensions only
print("\n💡 STRATEGY 3: Only hash specific high-value extensions")
print("-" * 80)

# Get extension distribution
cur.execute("""
    SELECT COALESCE(ext, '<no ext>') AS ext, COUNT(*) AS count
    FROM files
    WHERE state NOT IN ('error', 'missing')
      AND quick_hash IS NULL
      AND size_bytes >= ?
    GROUP BY COALESCE(ext, '')
    ORDER BY COUNT(*) DESC
    LIMIT 10
""", (small_file_threshold,))

print("\n  Top 10 extensions needing hashing:")
top_extensions = []
for ext, count in cur.fetchall():
    print(f"    {ext:>10}: {count:>7,} files")
    if ext != '<no ext>':
        top_extensions.append(ext)

# Test filtering to only high-value extensions
high_value_exts = ['.pdf', '.dwg', '.docx', '.xlsx']
placeholders = ','.join('?' * len(high_value_exts))

cur.execute(f"""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND LOWER(ext) IN ({placeholders})
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    )
    SELECT COUNT(*)
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.quick_hash IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= ?
      AND LOWER(f.ext) IN ({placeholders})
""", (*high_value_exts, small_file_threshold, *high_value_exts))

count = cur.fetchone()[0]
reduction = baseline - count
pct = (reduction / baseline * 100) if baseline > 0 else 0
print(f"\n  Only {', '.join(high_value_exts)}: {count:>7,} files  (saves {reduction:>7,} files, -{pct:.1f}%)")

# Strategy 4: Combined - min size 512KB + min group 3+ + high-value exts
print("\n💡 STRATEGY 4: COMBINED (512KB min + 3+ group + high-value exts)")
print("-" * 80)

cur.execute(f"""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND LOWER(ext) IN ({placeholders})
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) >= 3
    )
    SELECT COUNT(*)
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.quick_hash IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= 524288
      AND LOWER(f.ext) IN ({placeholders})
""", (*high_value_exts, *high_value_exts))

count = cur.fetchone()[0]
reduction = baseline - count
pct = (reduction / baseline * 100) if baseline > 0 else 0
print(f"  Combined filters: {count:>7,} files  (saves {reduction:>7,} files, -{pct:.1f}%)")

# Calculate potential time savings
print("\n" + "=" * 80)
print("⏱️  TIME SAVINGS ESTIMATE")
print("=" * 80)
print("\nAssuming 50 files/second processing rate:")
print(f"  Baseline:        {baseline:>7,} files = {baseline/50/60:.1f} minutes")
print(f"  512KB min:       {count:>7,} files = {count/50/60:.1f} minutes")
print(f"  Time saved:                      {(baseline-count)/50/60:.1f} minutes")

con.close()
