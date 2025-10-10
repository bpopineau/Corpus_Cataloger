"""Real-time progress monitor for throttled robocopy batches.

Usage
-----
Run from the project root while the throttled robocopy batch is executing::

    python scripts/monitor_robocopy.py --batch robocopy_throttled.bat --log robocopy_throttled.log

The script periodically reads the robocopy log, aggregates per-file summaries,
calculates throughput, and prints a live progress dashboard. Press Ctrl+C to
exit the monitor without affecting the copy.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

TIME_FORMAT = "%A, %B %d, %Y %I:%M:%S %p"
COLUMN_LABELS = ("total", "copied", "skipped", "mismatch", "failed", "extras")


@dataclass
class SummaryStats:
    """Aggregated counters parsed from the robocopy log."""

    processed: int = 0
    copied: int = 0
    skipped: int = 0
    mismatch: int = 0
    failed: int = 0
    extras: int = 0
    bytes_total: int = 0
    bytes_copied: int = 0
    bytes_skipped: int = 0
    bytes_mismatch: int = 0
    bytes_failed: int = 0
    bytes_extras: int = 0
    block_count: int = 0
    start_time: Optional[datetime] = None
    last_start: Optional[datetime] = None
    last_end: Optional[datetime] = None
    last_file: Optional[str] = None
    last_status: Optional[str] = None

    @property
    def processed_files(self) -> int:
        return self.copied + self.skipped + self.mismatch + self.failed + self.extras


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor throttled robocopy progress in real time")
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("robocopy_throttled.log"),
        help="Path to the robocopy log file (default: robocopy_throttled.log)",
    )
    parser.add_argument(
        "--batch",
        type=Path,
        default=Path("robocopy_throttled.bat"),
        help="Path to the generated robocopy batch file (default: robocopy_throttled.bat)",
    )
    parser.add_argument(
        "--total-files",
        type=int,
        default=None,
        help="Override total file count for percentage calculations",
    )
    parser.add_argument(
        "--total-size-bytes",
        type=int,
        default=None,
        help="Override total byte count for percentage calculations",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=5.0,
        help="Seconds between updates (default: 5)",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Append updates instead of clearing the screen",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print a single snapshot and exit",
    )
    return parser.parse_args()


def parse_datetime(value: str) -> Optional[datetime]:
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, TIME_FORMAT)
    except ValueError:
        return None


def parse_human_size(text: str) -> Optional[int]:
    text = text.strip()
    if not text:
        return None
    parts = text.split()
    if not parts:
        return None
    try:
        number = float(parts[0].replace(",", ""))
    except ValueError:
        return None
    unit = parts[1].upper() if len(parts) > 1 else "B"
    multiplier: Dict[str, float] = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }
    return int(number * multiplier.get(unit, 1))


def parse_batch_metadata(batch_path: Path) -> Dict[str, Optional[int]]:
    metadata: Dict[str, Optional[int]] = {"total_files": None, "total_bytes": None}
    if not batch_path.exists():
        return metadata

    try:
        with batch_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                upper = line.upper()
                if upper.startswith("REM TOTAL FILES:"):
                    number = ''.join(ch for ch in line.split(":", 1)[-1] if ch.isdigit())
                    if number:
                        metadata["total_files"] = int(number)
                elif upper.startswith("REM TOTAL SIZE:"):
                    size_part = line.split(":", 1)[-1].strip()
                    parsed = parse_human_size(size_part)
                    if parsed:
                        metadata["total_bytes"] = parsed
        return metadata
    except OSError:
        return metadata


def parse_log(log_path: Path) -> SummaryStats:
    stats = SummaryStats()
    if not log_path.exists():
        return stats

    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue

                if line.startswith("Started :"):
                    dt = parse_datetime(line.split(":", 1)[-1])
                    stats.last_start = dt
                    if stats.start_time is None and dt is not None:
                        stats.start_time = dt
                elif line.startswith("Bytes :"):
                    payload = line.split(":", 1)[-1].strip()
                    tokens = payload.split()
                    if tokens and all(token.isdigit() for token in tokens[: len(COLUMN_LABELS)]):
                        numbers = [int(token) for token in tokens[: len(COLUMN_LABELS)]]
                        numbers.extend([0] * (len(COLUMN_LABELS) - len(numbers)))
                        totals = dict(zip(COLUMN_LABELS, numbers))
                        stats.bytes_total += totals["total"]
                        stats.bytes_copied += totals["copied"]
                        stats.bytes_skipped += totals["skipped"]
                        stats.bytes_mismatch += totals["mismatch"]
                        stats.bytes_failed += totals["failed"]
                        stats.bytes_extras += totals["extras"]
                        stats.skipped += totals["skipped"]
                        stats.mismatch += totals["mismatch"]
                        stats.failed += totals["failed"]
                        stats.extras += totals["extras"]

                        if totals["copied"]:
                            stats.last_status = "copied"
                        elif totals["skipped"]:
                            stats.last_status = "skipped"
                        elif totals["failed"]:
                            stats.last_status = "failed"
                        elif totals["mismatch"]:
                            stats.last_status = "mismatch"
                        elif totals["extras"]:
                            stats.last_status = "extra"
                    elif payload:
                        stats.last_file = payload
                elif line.startswith("Bytes :"):
                    payload = line.split(":", 1)[-1].strip()
                    tokens = payload.split()
                    numbers = [int(token) for token in tokens[: len(COLUMN_LABELS)] if token.isdigit()]
                    numbers.extend([0] * (len(COLUMN_LABELS) - len(numbers)))
                    numbers.extend([0] * (len(COLUMN_LABELS) - len(numbers)))
                    totals = dict(zip(COLUMN_LABELS, numbers))
                    stats.bytes_total += totals["total"]
                    stats.bytes_copied += totals["copied"]
                    stats.bytes_skipped += totals["skipped"]
                    stats.bytes_mismatch += totals["mismatch"]
                    stats.bytes_failed += totals["failed"]
                    stats.bytes_extras += totals["extras"]
    except OSError:
        return stats

    return stats


def format_bytes(value: int) -> str:
    num = float(value)
    if num < 1024:
        return f"{int(num)} B"
    for unit in ("KB", "MB", "GB", "TB", "PB"):
        num /= 1024.0
        if num < 1024:
            return f"{num:.2f} {unit}"
    return f"{num:.2f} EB"


def format_duration(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def calculate_eta(bytes_done: int, bytes_total: Optional[int], start_time: Optional[datetime]) -> Optional[tuple[datetime, timedelta]]:
    if not start_time or not bytes_done or not bytes_total or bytes_total <= bytes_done:
        return None
    elapsed = datetime.now() - start_time
    seconds = elapsed.total_seconds()
    if seconds <= 0:
        return None
    rate = bytes_done / seconds
    if rate <= 0:
        return None
    remaining = (bytes_total - bytes_done) / rate
    eta = datetime.now() + timedelta(seconds=remaining)
    return eta, timedelta(seconds=int(remaining))


def print_snapshot(
    stats: SummaryStats,
    total_files: Optional[int],
    total_bytes: Optional[int],
    refresh: float,
    no_clear: bool,
) -> None:
    if not no_clear:
        os.system("cls" if os.name == "nt" else "clear")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Robocopy progress monitor @ {now}")
    print("=" * 80)

    processed = stats.processed_files
    if total_files:
        percent = (processed / total_files) * 100 if total_files else 0
        print(f"Files processed: {processed:,} / {total_files:,} ({percent:.2f}%)")
    else:
        print(f"Files processed: {processed:,}")

    print(f"  copied: {stats.copied:,}\n  skipped: {stats.skipped:,}\n  failed: {stats.failed:,}\n  mismatch: {stats.mismatch:,}\n  extras: {stats.extras:,}")

    if stats.bytes_copied or stats.bytes_skipped:
        copied_text = format_bytes(stats.bytes_copied)
        skipped_text = format_bytes(stats.bytes_skipped)
        total_text = format_bytes(stats.bytes_total)
        if total_bytes:
            bytes_percent = (stats.bytes_total / total_bytes) * 100 if total_bytes else 0
            print(f"Bytes processed: {total_text} of {format_bytes(total_bytes)} ({bytes_percent:.2f}%)")
        else:
            print(f"Bytes processed: {total_text}")
        print(f"  copied: {copied_text}\n  skipped: {skipped_text}")

    if stats.start_time:
        elapsed = datetime.now() - stats.start_time
        print(f"Elapsed: {format_duration(elapsed)}")
        if elapsed.total_seconds() > 0 and stats.bytes_copied:
            rate = stats.bytes_copied / elapsed.total_seconds()
            print(f"Average throughput: {format_bytes(int(rate))}/s")
        if stats.bytes_copied and total_bytes:
            eta_info = calculate_eta(stats.bytes_copied, total_bytes, stats.start_time)
            if eta_info:
                eta, remaining = eta_info
                print(f"Estimated completion: {eta.strftime('%Y-%m-%d %H:%M:%S')} ({format_duration(remaining)} remaining)")

    if stats.last_file:
        status = stats.last_status or ""
        status_display = f" [{status.upper()}]" if status else ""
        print(f"Last file: {stats.last_file}{status_display}")
        if stats.last_end:
            ago = datetime.now() - stats.last_end
            print(f"  finished: {stats.last_end.strftime('%Y-%m-%d %H:%M:%S')} ({format_duration(ago)} ago)")
    elif stats.block_count == 0:
        print("Waiting for robocopy to write to the log...")

    print("-" * 80)
    if not no_clear:
        print(f"Next update in {refresh:.1f}s (Ctrl+C to exit)")


def main() -> int:
    args = parse_args()

    batch_meta = parse_batch_metadata(args.batch)
    total_files = args.total_files or batch_meta.get("total_files")
    total_bytes = args.total_size_bytes or batch_meta.get("total_bytes")

    if not args.log.exists():
        print(f"Waiting for log file at {args.log} ...", file=sys.stderr)
        while not args.log.exists():
            try:
                time.sleep(max(args.refresh, 1.0))
            except KeyboardInterrupt:
                return 0

    try:
        while True:
            stats = parse_log(args.log)
            print_snapshot(stats, total_files, total_bytes, args.refresh, args.no_clear)
            if args.once:
                break
            time.sleep(args.refresh)
    except KeyboardInterrupt:
        print("\nMonitor stopped (robocopy continues in the background).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
