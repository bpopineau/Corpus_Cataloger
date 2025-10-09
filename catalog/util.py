from __future__ import annotations
from pathlib import Path
import hashlib
from typing import Any

_xxhash: Any
try:
    import xxhash as _xxhash
except Exception:
    _xxhash = None

xxhash: Any = _xxhash

def quick_hash(path: Path, head_tail_bytes: int = 65536) -> str:
    size = path.stat().st_size
    h = xxhash.xxh64() if xxhash else hashlib.sha1()
    h.update(str(size).encode())
    n = head_tail_bytes
    # Use buffered I/O for better throughput on Windows/network shares
    with open(path, "rb") as f:
        head = f.read(n)
        if head:
            h.update(head)
        if size > n:
            try:
                f.seek(max(0, size - n))
            except OSError:
                pass
            tail = f.read(n)
            if tail:
                h.update(tail)
    return h.hexdigest()

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    # Buffered I/O tends to perform better across platforms
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()
