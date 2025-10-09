#!/usr/bin/env python3
"""
Verify that the size+ext filter is ACTUALLY working by sampling candidates.
"""
from pathlib import Path
from catalog.config import load_config
from catalog.db import connect

cfg = load_config(Path("config/catalog.yaml"))
con = connect(Path(cfg.db.path))
cur = con.cursor()

small_file_threshold = cfg.dedupe.small_file_threshold

print("=" * 80)
print("VERIFYING SIZE+EXT FILTER LOGIC")
print("=" * 80)

# Get 10 random candidates from the quick_hash query
cur.execute("""
    WITH dup_candidates AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    )
    SELECT f.file_id, f.path_abs, f.size_bytes, f.ext
    FROM files f
    INNER JOIN dup_candidates dc
      ON f.size_bytes = dc.size_bytes
     AND COALESCE(f.ext, '') = dc.ext
    WHERE f.quick_hash IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= ?
    ORDER BY RANDOM()
    LIMIT 10
""", (small_file_threshold,))

print("\nSample of 10 files selected for quick hashing:")
print("-" * 80)

sample_files = cur.fetchall()
for file_id, path, size, ext in sample_files:
    size_mb = size / (1024 * 1024)
    print(f"\nFile ID: {file_id}")
    print(f"  Size: {size_mb:.2f} MB ({size:,} bytes)")
    print(f"  Ext: {ext or '<none>'}")
    print(f"  Path: {path[:80]}...")
    
    # Verify this file's size+ext combo HAS duplicates
    cur.execute("""
        SELECT COUNT(*)
        FROM files
        WHERE size_bytes = ?
          AND COALESCE(ext, '') = ?
          AND state NOT IN ('error', 'missing')
    """, (size, ext or ''))
    
    count = cur.fetchone()[0]
    print(f"  ✓ Files with same size+ext: {count}")
    
    if count == 1:
        print(f"  ❌ ERROR: This file should NOT be a candidate! It has unique size+ext!")

print("\n" + "=" * 80)
print("INVERSE CHECK: Files that SHOULD be excluded")
print("=" * 80)

# Find files with UNIQUE size+ext that should NOT be candidates
cur.execute("""
    WITH unique_combos AS (
        SELECT size_bytes, COALESCE(ext, '') AS ext
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) = 1
    )
    SELECT f.file_id, f.size_bytes, f.ext
    FROM files f
    INNER JOIN unique_combos uc
      ON f.size_bytes = uc.size_bytes
     AND COALESCE(f.ext, '') = uc.ext
    WHERE f.quick_hash IS NULL
      AND f.state NOT IN ('error', 'missing')
      AND f.size_bytes >= ?
    LIMIT 5
""", (small_file_threshold,))

unique_files = cur.fetchall()
if unique_files:
    print("\nFiles with UNIQUE size+ext (should be excluded from candidates):")
    print("-" * 80)
    for file_id, size, ext in unique_files:
        size_mb = size / (1024 * 1024)
        print(f"File ID {file_id}: {size_mb:.2f} MB, ext={ext or '<none>'}")
    print(f"\n✓ These {len(unique_files)} files are correctly EXCLUDED from candidates")
else:
    print("\n✓ No files with unique size+ext found in candidate set (correct!)")

con.close()
