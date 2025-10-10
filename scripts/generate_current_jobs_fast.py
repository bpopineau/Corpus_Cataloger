from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from catalog.db import connect

DB_PATH = Path("data/projects.db")
SOURCE_PREFIX = "S:\\1 Jobs\\1 Current Jobs\\"
DEST_ROOT = Path(r"C:\Users\brand\Projects\Server\1 Jobs\1 Current Jobs")
OUTPUT_FILE = Path("robocopy_current_jobs_fast.bat")
LOG_NAME = "robocopy_current_jobs_fast.log"
THREADS = 16


def main() -> None:
    con = connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT dir, name
        FROM files
        WHERE dir LIKE ?
          AND state NOT IN ('error', 'missing')
          AND path_abs LIKE ?
        ORDER BY dir, name
        """,
        (SOURCE_PREFIX + "%", SOURCE_PREFIX + "%"),
    )
    rows = cur.fetchall()
    con.close()

    dir_files: dict[str, list[str]] = defaultdict(list)
    for dir_path, filename in rows:
        dir_files[dir_path].append(filename)

    with OUTPUT_FILE.open("w", encoding="utf-8") as fh:
        fh.write("@echo off\n")
        fh.write("REM Unthrottled robocopy for Current Jobs\n")
        fh.write(f"REM Source: {SOURCE_PREFIX}\n")
        fh.write(f"REM Destination: {DEST_ROOT}\n")
        fh.write(f"REM Total files: {len(rows):,}\n")
        fh.write(f"REM Threads: {THREADS}\n")
        fh.write("echo Starting unthrottled copy of Current Jobs...\n")
        fh.write("echo Press Ctrl+C to stop.\n\n")

        total_dirs = len(dir_files)
        for idx, (dir_path, filenames) in enumerate(dir_files.items(), 1):
            rel_dir = dir_path[len(SOURCE_PREFIX) :]
            dest_dir = DEST_ROOT / rel_dir
            display = rel_dir or "(root)"
            fh.write(f"echo {idx}/{total_dirs}: {display}\n")
            src = dir_path.replace('"', '\\"')
            dst = str(dest_dir).replace('"', '\\"')
            for name in filenames:
                file_name = name.replace('"', '\\"')
                fh.write(
                    f"robocopy \"{src}\" \"{dst}\" \"{file_name}\" /MT:{THREADS} /R:2 /W:5 /NFL /NDL /NP /BYTES /TEE /LOG+:{LOG_NAME}\n"
                )
            fh.write("echo.\n")

        fh.write("echo Copy complete.\n")
        fh.write("pause\n")


if __name__ == "__main__":
    main()
