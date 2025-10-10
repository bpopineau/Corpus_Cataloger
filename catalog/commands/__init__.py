"""Command registration for the Corpus Cataloger CLI."""
from __future__ import annotations

from typing import Iterable

from . import dedupe, export, gui, hash_blake3, scan

COMMAND_MODULES: Iterable = (scan, hash_blake3, dedupe, export, gui)

__all__ = ["COMMAND_MODULES"]
