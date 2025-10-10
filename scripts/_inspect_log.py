from pathlib import Path
from monitor_robocopy import parse_log

stats = parse_log(Path("robocopy_throttled.log"))
print("blocks", stats.block_count)
print("copied", stats.copied)
print("skipped", stats.skipped)
print("failed", stats.failed)
print("processed property", stats.processed_files)
