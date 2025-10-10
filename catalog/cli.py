"""Unified command-line interface for Corpus Cataloger."""
from __future__ import annotations

import argparse
import sys
from typing import Optional, Sequence

from .commands import COMMAND_MODULES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="catalog",
        description="Corpus Cataloger command-line interface",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")
    for module in COMMAND_MODULES:
        module.add_parser(subparsers)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 1
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
