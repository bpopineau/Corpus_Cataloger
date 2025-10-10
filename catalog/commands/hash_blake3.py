"""CLI command for computing BLAKE3 hashes for catalog entries."""
from __future__ import annotations

import argparse
from argparse import _SubParsersAction
from pathlib import Path
from typing import Optional, Sequence

from ..config import load_config
from ..hash_all import hash_all_blake3


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    parser.add_argument("--force", action="store_true", help="Re-hash files even if a BLAKE3 digest exists")
    parser.add_argument("--max-workers", type=int, help="Override worker thread count")
    parser.add_argument("--include-prefix", action="append", help="Only process files with paths starting with this prefix (may repeat)")
    parser.add_argument("--exclude-prefix", action="append", help="Skip files with paths starting with this prefix (may repeat)")
    parser.add_argument("--io-bytes-per-sec", type=int, help="Approximate global I/O rate limit in bytes per second")
    parser.add_argument("--chunk-bytes", type=int, help="Chunk size (bytes) for streaming reads")
    parser.add_argument("--mirror-to-sha256", action="store_true", help="Copy the BLAKE3 digest into the sha256 column")


def add_parser(subparsers: _SubParsersAction[argparse.ArgumentParser]) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "hash",
        help="Compute BLAKE3 hashes for catalog entries",
        description="Stream cataloged files and generate BLAKE3 digests with optional throttling.",
    )
    _configure_parser(parser)
    parser.set_defaults(handler=run_from_args)
    return parser


def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog or "catalog hash", description="Compute BLAKE3 hashes for catalog entries")
    _configure_parser(parser)
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    cfg = load_config(Path(args.config))
    stats = hash_all_blake3(
        cfg,
        force=args.force,
        max_workers=args.max_workers,
        include_prefixes=args.include_prefix or [],
        exclude_prefixes=args.exclude_prefix or [],
        io_bytes_per_sec=args.io_bytes_per_sec,
        chunk_bytes=args.chunk_bytes,
        mirror_to_sha256=args.mirror_to_sha256,
    )

    print("\n" + "=" * 80)
    print("BLAKE3 HASH SUMMARY")
    print("=" * 80)
    print(f"Total candidates:    {stats.total_candidates:>10,}")
    print(f"Hashed:              {stats.hashed:>10,}")
    print(f"Skipped existing:    {stats.skipped_existing:>10,}")
    print(f"Missing files:       {stats.missing:>10,}")
    print(f"Errors:              {stats.errors:>10,}")
    print("=" * 80)
    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run_from_args(args)


__all__ = ["add_parser", "build_parser", "run_cli", "run_from_args"]
