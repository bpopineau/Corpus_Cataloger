"""CLI command for launching the desktop GUI."""
from __future__ import annotations

import argparse
import os
from argparse import _SubParsersAction
from typing import Optional, Sequence

try:  # pragma: no cover - optional dependency
    from .. import gui as gui_module
except Exception:  # pragma: no cover - import guarded to keep CLI usable without PySide6
    gui_module = None  # type: ignore


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/catalog.yaml", help="Catalog configuration file")


def add_parser(subparsers: _SubParsersAction[argparse.ArgumentParser]) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "gui",
        help="Launch the desktop GUI explorer",
        description="Open the Qt-based explorer for browsing catalog entries.",
    )
    _configure_parser(parser)
    parser.set_defaults(handler=run_from_args)
    return parser


def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog or "catalog gui", description="Launch the desktop GUI explorer")
    _configure_parser(parser)
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    if gui_module is None:
        raise SystemExit("GUI dependencies (PySide6) are not installed.")

    os.environ.setdefault("CATALOG_CONFIG_PATH", args.config)

    gui_module.main()
    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run_from_args(args)


__all__ = ["add_parser", "build_parser", "run_cli", "run_from_args"]
