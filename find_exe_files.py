"""Find all .exe files in the database."""
from catalog.db import connect
from pathlib import Path

db_path = Path("data/projects.db")
con = connect(db_path)
cur = con.cursor()

# Find all .exe files
cur.execute("""
    SELECT path_abs, size_bytes 
    FROM files 
    WHERE name LIKE '%.exe' 
      AND state NOT IN ('error', 'missing')
    ORDER BY size_bytes DESC
""")

results = cur.fetchall()

print("=" * 100)
print("EXE FILES IN DATABASE")
print("=" * 100)
print()
print(f"Total .exe files: {len(results)}")
print()

if results:
    print("Top 20 largest .exe files:")
    print()
    print(f"{'Size (MB)':>12}  {'Path'}")
    print("-" * 100)
    
    for path, size in results[:20]:
        size_mb = size / (1024 * 1024)
        print(f"{size_mb:>12.2f}  {path}")
    
    if len(results) > 20:
        print()
        print(f"... and {len(results) - 20} more .exe files")
    
    print()
    print("-" * 100)
    total_size = sum(size for _, size in results)
    total_size_gb = total_size / (1024 * 1024 * 1024)
    print(f"Total size of all .exe files: {total_size_gb:.2f} GB ({total_size:,} bytes)")
    print()
    
    # Show all .exe files
    print()
    print("ALL .EXE FILES:")
    print("-" * 100)
    for path, size in results:
        size_mb = size / (1024 * 1024)
        print(f"{size_mb:>12.2f} MB  {path}")

con.close()
