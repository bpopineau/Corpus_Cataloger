# Hash Performance Comparison

## Speed Comparison (approximate)

### Hashing Algorithms:
1. **BLAKE3** - ~3-10 GB/s (fastest, recommended)
2. **xxHash** - ~10 GB/s (extremely fast, but not cryptographic)
3. **SHA256** - ~300-500 MB/s (slow, cryptographically secure)
4. **SHA1** - ~600-800 MB/s (faster than SHA256, but deprecated)

## Current Setup

### Quick Hash (Stage 1 - Fast Filtering):
- Uses **xxHash64** (if available) or SHA1
- Only reads head + tail of files (default: 256 KB total)
- Purpose: Fast pre-filtering to group potential duplicates
- Speed: Very fast, processes thousands of files/sec

### Full Hash (Stage 2 - Verification):
- **With BLAKE3** (recommended): 
  - Computes both BLAKE3 and SHA256 in one pass
  - BLAKE3 is used for speed (~10x faster than SHA256 alone)
  - SHA256 is stored for verification/compatibility
  - Speed: ~5-15 files/sec for large files on network drives
  
- **Without BLAKE3**:
  - Only computes SHA256
  - Speed: ~1-3 files/sec for large files on network drives

## Installation

```powershell
# Install BLAKE3 for 10x faster hashing
pip install blake3
```

## Usage

```powershell
# Use BLAKE3 (recommended - much faster)
python run_hash_dedupe.py --blake3 --min-size 1024 --min-copies 2 --include-prefix "S:\"

# Progressive mode (for very large files on slow drives)
python run_hash_dedupe.py --blake3 --progressive --min-size 1024 --min-copies 2 --include-prefix "S:\"

# Network-friendly mode (lower concurrency, smaller chunks)
python run_hash_dedupe.py --blake3 --network-friendly --min-size 1024 --min-copies 2 --include-prefix "S:\"
```

## Performance Tips

1. **Use BLAKE3** - 10x speed improvement
2. **Adjust min-size** - Skip small files that don't matter (use --min-size 1048576 for >= 1MB)
3. **Network drives** - Use --network-friendly to reduce burst I/O
4. **Progressive mode** - For very large files, samples head/tail before full hash
5. **SSD vs Network** - Local SSD: 20-50 files/sec, Network: 5-15 files/sec

## Why Keep SHA256?

Even though BLAKE3 is faster, we also compute SHA256 because:
- Industry standard for file integrity
- Compatible with other tools
- Required for some compliance scenarios
- Provides cryptographic verification

With BLAKE3, we get:
- Speed of BLAKE3 for processing
- Security of SHA256 for verification
- Best of both worlds!
