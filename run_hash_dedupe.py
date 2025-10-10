"""Backward-compatible wrapper for the legacy hash dedupe script."""

from __future__ import annotations

import sys

from catalog.cli import main as catalog_main


def main(argv: list[str] | None = None) -> int:
    cli_args = ["dedupe"]
    if argv is None:
        argv = sys.argv[1:]
    cli_args.extend(argv)
    return catalog_main(cli_args)


if __name__ == "__main__":
    raise SystemExit(main())
