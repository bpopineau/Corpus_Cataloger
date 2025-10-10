# Local File Processing Workflow

This guide walks through the complete process of copying files locally and processing them for duplicate detection.

## Overview

1. **Copy files from network to local** (using robocopy)
2. **Purge the database** (clear old network paths)
3. **Scan local files** (rebuild catalog with local paths)
4. **Run metadata dedupe** (remove duplicates by size/name)
5. **Run hash dedupe with BLAKE3** (find byte-for-byte duplicates)

## Step 1: Copy Files Locally (IN PROGRESS)

**Status:** Robocopy is currently running in the background

**Command:**
```bash
robocopy "S:\" "C:\Users\brand\Projects\Server" /MIR /MT:8 /R:2 /W:5 /BYTES /TEE /LOG:robocopy_full.log
```

**What it does:**
- Mirrors all 65,408 files (238.49 GB) from S:\ to C:\Users\brand\Projects\Server
- Uses 8 threads for parallel copying
- Creates detailed log: `robocopy_full.log`
- Estimated time: ~20 minutes

**How to check progress:**
- Watch the terminal output
- Or check the log file: `type robocopy_full.log | Select-Object -Last 20`

**Wait for this to complete before proceeding to Step 2!**

---

## Step 2: Purge Database

Once the robocopy completes, clear the old database:

```bash
python purge_database.py
```

**What it does:**
- Removes all 65,408 file records pointing to S:\
- Removes all scan records
- Vacuums database to reclaim space
- Requires confirmation (type "PURGE")

**Output:**
- Shows count of deleted files and scans
- Database will be empty and ready for fresh scan

---

## Step 3: Scan Local Files

After purging, scan the new local files:

```bash
python scan_local_files.py
```

**What it does:**
- Scans C:\Users\brand\Projects\Server recursively
- Indexes all files with size, path, and metadata
- Builds fresh catalog database with local paths
- Much faster than network scan!

**Expected time:**
- ~5-10 minutes for 65,408 files (local SSD is fast!)

---

## Step 4: Metadata Dedupe

Remove duplicates that have the same size and filename:

```bash
python prune_metadata_dupes.py --min-size 1024 --min-copies 2
```

**What it does:**
- Finds files with identical size + name
- Minimum file size: 1 KB (1024 bytes)
- Minimum copies: 2 (any duplicate)
- Keeps oldest copy, marks others for deletion
- Interactive confirmation

**Previous results (on network scan):**
- Removed 29,849 duplicates
- Reduced from 95,260 to 65,408 files

---

## Step 5: Hash Dedupe with BLAKE3

Find byte-for-byte duplicates using cryptographic hashing:

```bash
python run_hash_dedupe.py --blake3 --min-size 1024 --min-copies 2
```

**What it does:**
- Uses BLAKE3 hash (10x faster than SHA256)
- Two-stage process:
  1. Quick hash (xxHash on first/last/middle chunks)
  2. Full hash (BLAKE3 on entire file) for matches
- Only hashes files with potential duplicates
- Generates duplicate report

**Expected performance:**
- Local SSD: ~5-15 files/sec (vs ~1 file/sec on network)
- Only ~3,580 files (16.90 GB) need full hashing
- Estimated time: ~10-30 minutes

**After completion:**
- Review duplicate groups
- Delete unwanted duplicates via GUI
- Or export report for review

---

## Benefits of Local Processing

### Speed Comparison

| Operation       | Network (S:\)    | Local (C:\)     | Speedup |
| --------------- | ---------------- | --------------- | ------- |
| Sequential read | ~100 MB/s        | ~500 MB/s       | 5x      |
| Random access   | 10-50 ms latency | <1 ms           | 10-50x  |
| Hash rate       | ~1 file/sec      | ~5-15 files/sec | 5-15x   |
| Small files     | Very slow        | Very fast       | 50-100x |

### Other Benefits

- ✅ No network interruptions
- ✅ Consistent performance
- ✅ Can work offline
- ✅ Faster database queries
- ✅ Faster GUI operations
- ✅ Can pause/resume without network issues

---

## Current Status

- ⏳ **Step 1:** Robocopy running (check terminal)
- ⏹️ **Step 2:** Ready to purge (run after Step 1 completes)
- ⏹️ **Step 3:** Ready to scan local files (run after Step 2)
- ⏹️ **Step 4:** Ready for metadata dedupe (run after Step 3)
- ⏹️ **Step 5:** Ready for hash dedupe (run after Step 4)

---

## Quick Reference Commands

```bash
# Check robocopy progress
type robocopy_full.log | Select-Object -Last 20

# Purge database (wait for robocopy first!)
python purge_database.py

# Scan local files
python scan_local_files.py

# Metadata dedupe
python prune_metadata_dupes.py --min-size 1024 --min-copies 2

# Hash dedupe with BLAKE3
python run_hash_dedupe.py --blake3 --min-size 1024 --min-copies 2

# Open GUI to review and delete duplicates
python -m catalog.gui
```

---

## Troubleshooting

### If robocopy fails
- Check disk space: Need 238.49 GB free
- Run as Administrator if permission errors
- Check log file: `robocopy_full.log`

### If scan finds no files
- Verify robocopy completed successfully
- Check path exists: `Test-Path "C:\Users\brand\Projects\Server"`
- Check folder contents: `Get-ChildItem "C:\Users\brand\Projects\Server" -Recurse | Measure-Object`

### If hash dedupe is still slow
- Check disk usage (Task Manager > Performance > Disk)
- Close other disk-intensive programs
- Try fewer workers: `--workers 4`
- Use network-friendly mode: `--network-friendly`
