#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to analyze file extensions and suggest candidates for deletion.

Improvements:
- Graceful Ctrl+C handling on Windows and Unix
- Avoids N+1 queries by collecting sample names in a single pass
- Optional limits to keep output manageable on very large datasets
"""

from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze file extensions in the catalog database")
    parser.add_argument("--db", default="data/projects.db", help="Path to SQLite database (default: data/projects.db)")
    parser.add_argument("--samples", type=int, default=3, help="Number of sample filenames to show per extension (default: 3)")
    parser.add_argument("--top", type=int, default=None, help="Only show the top N extensions by file count")
    parser.add_argument("--no-categories", action="store_true", help="Skip categorized analysis section")
    parser.add_argument("--quiet", action="store_true", help="Reduce verbosity")
    args = parser.parse_args(argv)

    db_path = Path(args.db)

    if not args.quiet:
        print("File Extension Analysis")
        print("=" * 80)

    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 1

    # SIGINT friendly: set a flag we can check from progress handler
    cancelled = {"flag": False}

    def _handle_sigint(signum, frame):  # noqa: ARG001
        cancelled["flag"] = True

    # Install Ctrl+C handler
    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        with sqlite3.connect(str(db_path)) as con:
            con.row_factory = sqlite3.Row

            # Make long-running queries interruptible
            def _progress_handler():
                # Return non-zero to abort the current SQLite operation
                return 1 if cancelled["flag"] else 0

            # Call progress handler periodically (every N SQLite VM steps)
            con.set_progress_handler(_progress_handler, 10_000)

            cur = con.cursor()

            # Total file count
            cur.execute("SELECT COUNT(*) FROM files")
            total_files = int(cur.fetchone()[0])
            if not args.quiet:
                print(f"Total files: {total_files:,}\n")

            if cancelled["flag"]:
                print("Cancelled.")
                return 130  # 128 + SIGINT

            # Extension counts
            if not args.quiet:
                print("All File Extensions (with counts and samples):")
                print("-" * 80)

            cur.execute(
                """
                SELECT ext, COUNT(*) as count
                FROM files
                WHERE ext IS NOT NULL
                GROUP BY ext
                ORDER BY count DESC
                """
            )

            extensions = cur.fetchall()
            if args.top is not None:
                extensions = extensions[: args.top]

            # Compute percentages and prepare ext list
            extensions_data = []
            ext_keys = []
            for row in extensions:
                ext = row[0]
                count = int(row[1])
                percentage = (count / total_files * 100) if total_files else 0.0
                extensions_data.append({"ext": ext, "count": count, "percentage": percentage, "samples": []})
                ext_keys.append(ext)

            if cancelled["flag"]:
                print("Cancelled.")
                return 130

            # Collect sample names in a single pass over files to avoid N+1 queries.
            # Restrict to extensions we plan to show (especially when --top is used).
            samples_by_ext: Dict[str, List[str]] = defaultdict(list)
            if ext_keys:
                # Build a dynamic parameter list for IN clause
                placeholders = ",".join(["?"] * len(ext_keys))
                sample_sql = (
                    f"SELECT ext, name FROM files WHERE ext IN ({placeholders}) ORDER BY rowid"
                )
                cur.execute(sample_sql, ext_keys)
                for ext, name in cur:
                    if cancelled["flag"]:
                        print("Cancelled.")
                        return 130
                    bucket = samples_by_ext[ext]
                    if len(bucket) < args.samples:
                        bucket.append(name)

            # Attach samples and print
            for d in extensions_data:
                d["samples"] = samples_by_ext.get(d["ext"], [])
                print(f"\n{d['ext']}: {d['count']:,} files ({d['percentage']:.2f}%)")
                print("  Samples:")
                for s in d["samples"]:
                    print(f"    - {s}")

            if cancelled["flag"]:
                print("Cancelled.")
                return 130

            if not args.no_categories:
                # Categorize extensions
                print("\n" + "=" * 80)
                print("CATEGORIZED ANALYSIS")
                print("=" * 80)

                categories = {
                    "Documents (Keep)": {
                        "exts": [
                            ".pdf",
                            ".docx",
                            ".doc",
                            ".xlsx",
                            ".xls",
                            ".pptx",
                            ".ppt",
                            ".txt",
                            ".rtf",
                            ".odt",
                        ],
                        "description": "Standard office documents - typically important",
                    },
                    "CAD/Technical (Keep)": {
                        "exts": [
                            ".dwg",
                            ".dxf",
                            ".skp",
                            ".rvt",
                            ".rfa",
                            ".ifc",
                            ".stp",
                            ".step",
                        ],
                        "description": "CAD and technical drawings - typically important",
                    },
                    "Email (Consider)": {
                        "exts": [".msg", ".eml", ".pst", ".ost"],
                        "description": "Email files - may be archival, consider if needed",
                    },
                    "Temporary/Cache (Delete)": {
                        "exts": [
                            ".tmp",
                            ".temp",
                            ".cache",
                            ".bak",
                            ".old",
                            "~",
                            ".crdownload",
                            ".part",
                        ],
                        "description": "Temporary files - usually safe to delete",
                    },
                    "System/Hidden (Delete)": {
                        "exts": [".db", ".ini", ".dat", ".log", ".lock", ".dll", ".sys"],
                        "description": "System files - often not needed for document management",
                    },
                    "Backup/Archive (Consider)": {
                        "exts": [".bak", ".backup", ".old", ".orig"],
                        "description": "Backup versions - may be redundant",
                    },
                    "Compressed (Keep/Consider)": {
                        "exts": [".zip", ".rar", ".7z", ".tar", ".gz"],
                        "description": "Archives - may contain important files",
                    },
                }

                for category, info in categories.items():
                    matching = [d for d in extensions_data if (d["ext"] or "").lower() in info["exts"]]
                    if matching:
                        print(f"\n{category}:")
                        print(f"  {info['description']}")
                        total_in_category = sum(d["count"] for d in matching)
                        print(f"  Total: {total_in_category:,} files")
                        for d in matching:
                            print(f"    {d['ext']}: {d['count']:,} ({d['percentage']:.2f}%)")

            # Find files without extensions
            cur.execute("SELECT COUNT(*) FROM files WHERE ext IS NULL OR ext = ''")
            no_ext_count = int(cur.fetchone()[0])
            if no_ext_count > 0:
                print(f"\nFiles without extension: {no_ext_count:,}")
                cur.execute("SELECT name FROM files WHERE ext IS NULL OR ext = '' LIMIT 5")
                print("  Samples:")
                for row in cur.fetchall():
                    print(f"    - {row[0]}")

    except KeyboardInterrupt:
        print("\nCancelled by user.")
        return 130
    except sqlite3.DatabaseError as e:
        if "interrupted" in str(e).lower():
            print("\nCancelled (SQLite interrupted).")
            return 130
        raise

    print("\n" + "=" * 80)
    print("Analysis complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
