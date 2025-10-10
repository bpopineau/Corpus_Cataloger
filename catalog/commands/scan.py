"""CLI command for scanning filesystem roots into the catalog."""
from __future__ import annotations

import argparse
from argparse import _SubParsersAction
from pathlib import Path
from typing import Optional, Sequence

from ..config import load_config
from ..scan import scan_root


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")
    parser.add_argument("--max-workers", type=int, help="Override scanner worker count")
    parser.add_argument("--root", action="append", help="Additional root path to scan (may repeat)")


def add_parser(subparsers: _SubParsersAction[argparse.ArgumentParser]) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "scan",
        help="Scan filesystem roots into the catalog",
        description="Enumerate configured roots and record metadata in the catalog database.",
    )
    _configure_parser(parser)
    parser.set_defaults(handler=run_from_args)
    return parser


def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog or "catalog scan", description="Scan filesystem roots into the catalog database")
    _configure_parser(parser)
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    cfg = load_config(Path(args.config))
    if args.max_workers is not None:
        cfg.scanner.max_workers = args.max_workers

    roots = list(cfg.roots)
    if args.root:
        roots.extend(args.root)

    if not roots:
        raise SystemExit("No root paths configured. Provide --root or configure cfg.roots.")

    for root in roots:
        print(f"[RUN] scanning root: {root}")
        scan_root(str(root), cfg)
    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run_from_args(args)


__all__ = ["add_parser", "build_parser", "run_cli", "run_from_args"]
