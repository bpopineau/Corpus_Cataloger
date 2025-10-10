"""CLI command for exporting catalog tables to Parquet."""
from __future__ import annotations

import argparse
from argparse import _SubParsersAction
from pathlib import Path
from typing import Optional, Sequence

from ..config import load_config
from ..export import main as export_main


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file (used when --db omitted)")
    parser.add_argument("--db", help="Explicit path to catalog SQLite database")
    parser.add_argument("--out", default="data/parquet", help="Destination folder for Parquet files")


def add_parser(subparsers: _SubParsersAction[argparse.ArgumentParser]) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "export",
        help="Export catalog tables to Parquet",
        description="Write catalog tables to Parquet files for downstream analytics.",
    )
    _configure_parser(parser)
    parser.set_defaults(handler=run_from_args)
    return parser


def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog or "catalog export", description="Export catalog tables to Parquet")
    _configure_parser(parser)
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    db_path = args.db
    if not db_path:
        cfg = load_config(Path(args.config))
        db_path = cfg.db.path
    export_args = ["--db", db_path, "--out", args.out]
    export_main(export_args)
    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run_from_args(args)


__all__ = ["add_parser", "build_parser", "run_cli", "run_from_args"]
