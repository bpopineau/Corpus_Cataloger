try:
    import blake3
    print("✓ BLAKE3 is installed")
    print("  BLAKE3 is ~10x faster than SHA256")
    print("  Use --blake3 flag to enable it")
except ImportError:
    print("✗ BLAKE3 is NOT installed")
    print("  Install with: pip install blake3")
    print("  BLAKE3 is ~10x faster than SHA256 for duplicate detection")

try:
    from catalog.util import xxhash
    print("✓ xxHash is available (used for quick hashing)")
except ImportError:
    print("✗ xxHash not available (will use SHA1 for quick hashing)")
