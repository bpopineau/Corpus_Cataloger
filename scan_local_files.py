"""
Scan local files after they've been copied from network drive.
This script will scan the local copy instead of the network drive.
"""
import subprocess
import sys
from pathlib import Path

def main():
    print("=" * 100)
    print("LOCAL FILE SCAN")
    print("=" * 100)
    print()
    
    local_path = Path(r"C:\Users\brand\Projects\Server")
    
    if not local_path.exists():
        print(f"❌ Error: Local path does not exist: {local_path}")
        print()
        print("Make sure the robocopy operation has completed first!")
        return 1
    
    print(f"Scanning: {local_path}")
    print()
    print("This will scan all files and build the catalog database.")
    print("The scan will index file paths, sizes, and metadata.")
    print()
    
    # Check if database exists and has data
    db_path = Path("data/projects.db")
    if db_path.exists():
        import sqlite3
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        file_count = cur.fetchone()[0]
        con.close()
        
        if file_count > 0:
            print(f"⚠️  WARNING: Database currently contains {file_count:,} files!")
            print()
            print("You should purge the database first using:")
            print("  python purge_database.py")
            print()
            response = input("Continue anyway? (yes/no): ").strip().lower()
            if response != "yes":
                print("Scan cancelled.")
                return 0
            print()
    
    # Run the scan
    print("Starting scan...")
    print()
    
    cmd = [
        sys.executable,
        "-m", "catalog.scan",
        "--config", "config/catalog.yaml",
        str(local_path)
    ]
    
    result = subprocess.run(cmd, check=False)
    
    print()
    if result.returncode == 0:
        print("=" * 100)
        print("✅ SCAN COMPLETE")
        print("=" * 100)
        print()
        print("The catalog database now contains the local files.")
        print()
        print("Next steps:")
        print("1. Run metadata dedupe:")
        print("   python prune_metadata_dupes.py --min-size 1024 --min-copies 2")
        print()
        print("2. Run hash-based dedupe:")
        print("   python run_hash_dedupe.py --blake3 --min-size 1024 --min-copies 2")
        print()
    else:
        print("❌ Scan failed with error code:", result.returncode)
        return result.returncode
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
