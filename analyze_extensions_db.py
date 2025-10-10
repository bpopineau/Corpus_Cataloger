"""Analyze file extensions in the database."""
from catalog.db import connect
from pathlib import Path
from collections import Counter

db_path = Path("data/projects.db")
con = connect(db_path)
cur = con.cursor()

# Get all file extensions
cur.execute("""
    SELECT name, size_bytes 
    FROM files 
    WHERE state NOT IN ('error', 'missing')
""")

results = cur.fetchall()

# Extract extensions
extensions = Counter()
extension_sizes = {}

for name, size in results:
    ext = Path(name).suffix.lower()
    if not ext:
        ext = "(no extension)"
    extensions[ext] += 1
    extension_sizes[ext] = extension_sizes.get(ext, 0) + size

print("=" * 100)
print("FILE EXTENSIONS IN DATABASE")
print("=" * 100)
print()
print(f"Total files: {len(results):,}")
print(f"Total extensions: {len(extensions)}")
print()
print(f"{'Extension':<20} {'Count':>10}  {'Total Size':>15}  {'Avg Size':>15}")
print("-" * 100)

# Sort by count
for ext, count in extensions.most_common(30):
    total_size = extension_sizes[ext]
    avg_size = total_size / count
    total_size_str = f"{total_size / (1024**2):.1f} MB" if total_size < 1024**3 else f"{total_size / (1024**3):.2f} GB"
    avg_size_str = f"{avg_size / 1024:.1f} KB" if avg_size < 1024**2 else f"{avg_size / (1024**2):.1f} MB"
    print(f"{ext:<20} {count:>10,}  {total_size_str:>15}  {avg_size_str:>15}")

print()
print("Note: .exe files are not in the current database catalog.")

con.close()
