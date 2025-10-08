from __future__ import annotations
from pathlib import Path
import hashlib

try:
    import xxhash
except Exception:
    xxhash = None

def quick_hash(path: Path, head_tail_bytes: int = 65536) -> str:
    size = path.stat().st_size
    h = xxhash.xxh64() if xxhash else hashlib.sha1()
    h.update(str(size).encode())
    n = head_tail_bytes
    with open(path, "rb", buffering=0) as f:
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
    with open(path, "rb", buffering=0) as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()
