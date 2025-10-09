#!/usr/bin/env python3
"""
Analyze ALL files needing hashing (quick or SHA256) and optimization opportunities.
"""
from pathlib import Path
from catalog.config import load_config
from catalog.db import connect

cfg = load_config(Path("config/catalog.yaml"))
con = connect(Path(cfg.db.path))
cur = con.cursor()

small_file_threshold = cfg.dedupe.small_file_threshold

print("=" * 80)
print("COMPLETE DEDUPE WORKLOAD ANALYSIS")
print("=" * 80)

# Files needing ANY kind of hash
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
    WHERE f.sha256 IS NULL
      AND f.state NOT IN ('error', 'missing')
""")

baseline_all = cur.fetchone()[0]
print(f"\n📊 BASELINE: {baseline_all:,} files need SHA256 hashing")
print("=" * 80)

# Break down by size
cur.execute("""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    )
    SELECT 
        CASE 
            WHEN f.size_bytes < 131072 THEN 'Small (< 128KB)'
            WHEN f.size_bytes < 524288 THEN 'Medium (128KB-512KB)'
            WHEN f.size_bytes < 2097152 THEN 'Large (512KB-2MB)'
            ELSE 'Very Large (> 2MB)'
        END AS size_category,
        COUNT(*) AS file_count
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.sha256 IS NULL
      AND f.state NOT IN ('error', 'missing')
    GROUP BY size_category
    ORDER BY 
        CASE size_category
            WHEN 'Small (< 128KB)' THEN 1
            WHEN 'Medium (128KB-512KB)' THEN 2
            WHEN 'Large (512KB-2MB)' THEN 3
            ELSE 4
        END
""")

print("\n📈 SIZE DISTRIBUTION:")
print("-" * 80)
for category, count in cur.fetchall():
    pct = (count / baseline_all * 100) if baseline_all > 0 else 0
    print(f"  {category:25} {count:>7,} files ({pct:>5.1f}%)")

# Strategy 1: Skip files below certain sizes
print("\n\n💡 STRATEGY 1: Set minimum file size")
print("-" * 80)

for min_size_kb in [128, 256, 512, 1024]:
    min_size_bytes = min_size_kb * 1024
    cur.execute("""
        WITH dup_candidates AS (
            SELECT size_bytes, COALESCE(ext, '') AS ext
            FROM files
            WHERE state NOT IN ('error', 'missing')
              AND size_bytes >= ?
            GROUP BY size_bytes, COALESCE(ext, '')
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*)
        FROM files f
        INNER JOIN dup_candidates dc
          ON f.size_bytes = dc.size_bytes
         AND COALESCE(f.ext, '') = dc.ext
        WHERE f.sha256 IS NULL
          AND f.state NOT IN ('error', 'missing')
          AND f.size_bytes >= ?
    """, (min_size_bytes, min_size_bytes))
    
    count = cur.fetchone()[0]
    reduction = baseline_all - count
    pct = (reduction / baseline_all * 100) if baseline_all > 0 else 0
    print(f"  Skip files < {min_size_kb:>4} KB: {count:>7,} files remain (saves {reduction:>7,}, -{pct:>5.1f}%)")

# Strategy 2: Require larger groups
print("\n💡 STRATEGY 2: Only hash larger duplicate groups")
print("-" * 80)

for min_group in [3, 5, 10, 20]:
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
        WHERE f.sha256 IS NULL
          AND f.state NOT IN ('error', 'missing')
    """, (min_group,))
    
    count = cur.fetchone()[0]
    reduction = baseline_all - count
    pct = (reduction / baseline_all * 100) if baseline_all > 0 else 0
    print(f"  Min group size {min_group:>2}: {count:>7,} files remain (saves {reduction:>7,}, -{pct:>5.1f}%)")

# Strategy 3: Extension filtering
print("\n💡 STRATEGY 3: Focus on high-value extensions")
print("-" * 80)

high_value = ['.pdf', '.dwg', '.docx', '.xlsx']
placeholders = ','.join('?' * len(high_value))

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
    WHERE f.sha256 IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND LOWER(f.ext) IN ({placeholders})
""", (*high_value, *high_value))

count = cur.fetchone()[0]
reduction = baseline_all - count
pct = (reduction / baseline_all * 100) if baseline_all > 0 else 0
print(f"  Only {', '.join(high_value)}: {count:>7,} files ({pct:>5.1f}% of baseline)")
print(f"  Skips non-valuable types: saves {reduction:>7,} files")

# RECOMMENDED: Combined approach
print("\n💡 RECOMMENDED: Combined Strategy")
print("-" * 80)
print("  - Skip files < 256 KB (small files rarely worth finding duplicates)")
print("  - Require 3+ files per size+ext group (focus on clusters)")
print("  - Focus on .pdf, .dwg, .docx, .xlsx only")

cur.execute(f"""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
          AND size_bytes >= 262144
          AND LOWER(ext) IN ({placeholders})
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) >= 3
    )
    SELECT COUNT(*)
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.sha256 IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= 262144
      AND LOWER(f.ext) IN ({placeholders})
""", (*high_value, *high_value))

recommended_count = cur.fetchone()[0]
reduction = baseline_all - recommended_count
pct = (reduction / baseline_all * 100) if baseline_all > 0 else 0

print(f"\n  Files to hash: {recommended_count:>7,}")
print(f"  Files saved:   {reduction:>7,} ({pct:.1f}% reduction)")
print(f"\n  Estimated time @ 50 files/sec:")
print(f"    Before: {baseline_all/50/60:.1f} minutes")
print(f"    After:  {recommended_count/50/60:.1f} minutes")
print(f"    Saved:  {reduction/50/60:.1f} minutes")

con.close()
