"""Quick script to check for metadata duplicates with lower thresholds."""
from catalog.config import load_config
from catalog.dedupe import detect_duplicates
from pathlib import Path

cfg = load_config(Path('config/catalog.yaml'))

# Lower the thresholds to catch more duplicates
cfg.dedupe.min_file_size = 1024  # 1 KB minimum
cfg.dedupe.min_duplicate_count = 2  # Only need 2 files to be a duplicate

print("Running metadata dedupe with:")
print(f"  - Min file size: {cfg.dedupe.min_file_size:,} bytes")
print(f"  - Min duplicate count: {cfg.dedupe.min_duplicate_count}")
print()

stats = detect_duplicates(
    cfg,
    metadata_only=True,
    include_prefixes=['S:\\'],
)

print(f'\n\nFound {stats["duplicate_groups"]} groups with {stats["duplicate_files"]} duplicate files')

groups = stats.get('metadata_groups', [])
print(f'\nTop 20 duplicate groups by wasted space:')
print('=' * 100)

for i, g in enumerate(groups[:20], 1):
    size_mb = g["size_bytes"] / (1024**2)
    wasted_mb = size_mb * (len(g["members"]) - 1)
    print(f'{i}. {g["name"]} ({g["ext"] or "no ext"})')
    print(f'   {len(g["members"])} copies × {size_mb:.2f} MB = {wasted_mb:.2f} MB wasted')
    print(f'   Paths:')
    for member in g["members"][:3]:
        print(f'     - {member["path"]}')
    if len(g["members"]) > 3:
        print(f'     ... and {len(g["members"]) - 3} more')
    print()

# Summary
total_wasted_bytes = sum(
    g["size_bytes"] * (len(g["members"]) - 1)
    for g in groups
)
print(f'\nTotal wasted space: {total_wasted_bytes / (1024**3):.2f} GB')
