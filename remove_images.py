#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to remove all image files from the database.
"""

import sqlite3
from pathlib import Path

db_path = Path("data/projects.db")

# Common image file extensions
IMAGE_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif',
    '.webp', '.svg', '.ico', '.heic', '.heif', '.raw', '.cr2',
    '.nef', '.orf', '.sr2', '.psd', '.ai', '.eps', '.indd'
}

print("Image File Removal Script")
print("=" * 60)

if not db_path.exists():
    print(f"ERROR: Database not found: {db_path}")
    exit(1)

with sqlite3.connect(str(db_path)) as con:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    
    # First, let's see what image extensions actually exist
    print("\n1. Analyzing database for image files...")
    print("-" * 60)
    
    cur.execute("SELECT DISTINCT ext FROM files WHERE ext IS NOT NULL ORDER BY ext")
    all_extensions = [row[0].lower() for row in cur.fetchall()]
    
    found_image_exts = [ext for ext in all_extensions if ext in IMAGE_EXTENSIONS]
    
    print(f"Total unique extensions in database: {len(all_extensions)}")
    print(f"Image extensions found: {len(found_image_exts)}")
    
    if found_image_exts:
        print("\nImage extensions present:")
        for ext in sorted(found_image_exts):
            cur.execute("SELECT COUNT(*) FROM files WHERE LOWER(ext) = ?", (ext,))
            count = cur.fetchone()[0]
            print(f"  {ext}: {count:,} files")
    
    # Count total image files
    placeholders = ','.join(['?' for _ in found_image_exts])
    if found_image_exts:
        count_query = f"SELECT COUNT(*) FROM files WHERE LOWER(ext) IN ({placeholders})"
        cur.execute(count_query, found_image_exts)
        total_images = cur.fetchone()[0]
    else:
        total_images = 0
    
    print(f"\nTotal image files to remove: {total_images:,}")
    
    if total_images == 0:
        print("\nNo image files found in database.")
        exit(0)
    
    # Get total files before deletion
    cur.execute("SELECT COUNT(*) FROM files")
    total_before = cur.fetchone()[0]
    print(f"Total files before deletion: {total_before:,}")
    
    # Confirm deletion
    print("\n2. Removing image files...")
    print("-" * 60)
    
    # Delete image files
    delete_query = f"DELETE FROM files WHERE LOWER(ext) IN ({placeholders})"
    cur.execute(delete_query, found_image_exts)
    deleted_count = cur.rowcount
    
    con.commit()
    
    # Get total files after deletion
    cur.execute("SELECT COUNT(*) FROM files")
    total_after = cur.fetchone()[0]
    
    print(f"Files deleted: {deleted_count:,}")
    print(f"Total files after deletion: {total_after:,}")
    print(f"Percentage removed: {(deleted_count / total_before * 100):.2f}%")
    
    # Verify deletion
    print("\n3. Verification...")
    print("-" * 60)
    
    if found_image_exts:
        verify_query = f"SELECT COUNT(*) FROM files WHERE LOWER(ext) IN ({placeholders})"
        cur.execute(verify_query, found_image_exts)
        remaining = cur.fetchone()[0]
        
        if remaining == 0:
            print("OK: All image files successfully removed")
        else:
            print(f"WARNING: {remaining} image files still remain")
    
    # Show current extension distribution
    print("\n4. Remaining file types (top 10)...")
    print("-" * 60)
    
    cur.execute("""
        SELECT ext, COUNT(*) as count 
        FROM files 
        WHERE ext IS NOT NULL 
        GROUP BY ext 
        ORDER BY count DESC 
        LIMIT 10
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,} files")
    
    # Optimize database after deletion
    print("\n5. Optimizing database...")
    print("-" * 60)
    print("Running VACUUM to reclaim space...")
    con.execute("VACUUM")
    print("Database optimized")

print("\n" + "=" * 60)
print("Image file removal complete!")
