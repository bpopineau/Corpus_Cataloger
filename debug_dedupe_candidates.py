#!/usr/bin/env python3
"""
Debug script to examine dedupe candidate selection and understand why filtering isn't working.
"""
from pathlib import Path
from catalog.config import load_config
from catalog.db import connect

cfg = load_config(Path("config/catalog.yaml"))
con = connect(Path(cfg.db.path))
cur = con.cursor()

print("=" * 80)
print("DEDUPE CANDIDATE ANALYSIS")
print("=" * 80)

# 1. Total active files
cur.execute("SELECT COUNT(*) FROM files WHERE state NOT IN ('error', 'missing')")
total_active = cur.fetchone()[0]
print(f"\n1. Total active files: {total_active:,}")

# 2. Files already with quick_hash
cur.execute("SELECT COUNT(*) FROM files WHERE quick_hash IS NOT NULL AND state NOT IN ('error', 'missing')")
already_quick_hashed = cur.fetchone()[0]
print(f"2. Files with quick_hash: {already_quick_hashed:,}")

# 3. Files already with sha256
cur.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL AND state NOT IN ('error', 'missing')")
already_sha256 = cur.fetchone()[0]
print(f"3. Files with SHA256: {already_sha256:,}")

# 4. Count size+ext groups with multiple files
cur.execute("""
    SELECT COUNT(*)
    FROM (
        SELECT size_bytes, COALESCE(ext, '') AS ext, COUNT(*) AS cnt
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    ) t
""")
duplicate_groups = cur.fetchone()[0]
print(f"\n4. Size+ext groups with 2+ files: {duplicate_groups:,}")

# 5. Total files in those duplicate groups
cur.execute("""
    SELECT COALESCE(SUM(cnt), 0)
    FROM (
        SELECT COUNT(*) AS cnt
        FROM files
        WHERE state NOT IN ('error', 'missing')
        GROUP BY size_bytes, COALESCE(ext, '')
        HAVING COUNT(*) > 1
    ) t
""")
duplicate_population = cur.fetchone()[0]
print(f"5. Files in duplicate size+ext groups: {duplicate_population:,}")

# 6. Files that SHOULD be quick-hashed (duplicate size+ext, no quick_hash, large enough)
small_file_threshold = cfg.dedupe.small_file_threshold
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
quick_hash_candidates = cur.fetchone()[0]
print(f"\n6. Files needing quick_hash (size+ext dup, no hash, >= {small_file_threshold:,} bytes): {quick_hash_candidates:,}")

# 7. Show top 10 size+ext groups
print("\n" + "=" * 80)
print("TOP 10 SIZE+EXT GROUPS WITH MOST FILES")
print("=" * 80)
cur.execute("""
    SELECT 
        size_bytes, 
        COALESCE(ext, '<no ext>') AS ext,
        COUNT(*) AS file_count,
        SUM(CASE WHEN quick_hash IS NULL THEN 1 ELSE 0 END) AS need_quick_hash,
        SUM(CASE WHEN sha256 IS NULL THEN 1 ELSE 0 END) AS need_sha256
    FROM files
    WHERE state NOT IN ('error', 'missing')
    GROUP BY size_bytes, COALESCE(ext, '')
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 10
""")

for row in cur.fetchall():
    size_mb = row[0] / (1024 * 1024)
    print(f"\n  Size: {size_mb:.2f} MB | Ext: {row[1]}")
    print(f"    Total files: {row[2]:,}")
    print(f"    Need quick_hash: {row[3]:,}")
    print(f"    Need SHA256: {row[4]:,}")

# 8. Check if most files are ALREADY hashed
print("\n" + "=" * 80)
print("HASHING STATUS BREAKDOWN")
print("=" * 80)

cur.execute("""
    SELECT 
        CASE 
            WHEN quick_hash IS NULL AND sha256 IS NULL THEN 'No hashes'
            WHEN quick_hash IS NOT NULL AND sha256 IS NULL THEN 'Quick hash only'
            WHEN sha256 IS NOT NULL THEN 'SHA256 (complete)'
            ELSE 'Other'
        END AS status,
        COUNT(*) AS count
    FROM files
    WHERE state NOT IN ('error', 'missing')
    GROUP BY status
    ORDER BY count DESC
""")

for row in cur.fetchall():
    print(f"  {row[0]:<20} {row[1]:>10,} files")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print(f"If quick_hash_candidates ({quick_hash_candidates:,}) ≈ total_active ({total_active:,}),")
print("then the size+ext filter ISN'T working.")
print(f"\nIt SHOULD be much smaller (ideally close to {duplicate_population:,}).")

con.close()
