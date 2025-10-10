"""
Purge all file data from the database and reset it for a fresh scan.
This will remove all scans and files, keeping only the database structure.
"""
import argparse
from pathlib import Path
from catalog.db import connect

def purge_database(db_path):
    """Remove all scan and file data from the database."""
    
    print("=" * 100)
    print("DATABASE PURGE UTILITY")
    print("=" * 100)
    print()
    print(f"Database: {db_path}")
    print()
    
    con = connect(db_path)
    cur = con.cursor()
    
    # Get current counts
    cur.execute("SELECT COUNT(*) FROM files")
    file_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM scans")
    scan_count = cur.fetchone()[0]
    
    print(f"Current database contents:")
    print(f"  Files: {file_count:,}")
    print(f"  Scans: {scan_count:,}")
    print()
    
    if file_count == 0 and scan_count == 0:
        print("Database is already empty. Nothing to purge.")
        con.close()
        return
    
    # Confirm purge
    print("⚠️  WARNING: This will DELETE ALL file and scan data from the database!")
    print("⚠️  This action CANNOT be undone!")
    print()
    response = input("Type 'PURGE' to confirm deletion: ").strip()
    
    if response != "PURGE":
        print()
        print("❌ Purge cancelled. No changes made.")
        con.close()
        return
    
    print()
    print("Purging database...")
    
    # Delete all files
    print(f"  Deleting {file_count:,} files...")
    cur.execute("DELETE FROM files")
    
    # Delete all scans
    print(f"  Deleting {scan_count:,} scans...")
    cur.execute("DELETE FROM scans")
    
    # Commit changes
    con.commit()
    
    # Verify deletion
    cur.execute("SELECT COUNT(*) FROM files")
    remaining_files = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM scans")
    remaining_scans = cur.fetchone()[0]
    
    # Vacuum to reclaim space
    print("  Vacuuming database to reclaim space...")
    cur.execute("VACUUM")
    
    con.close()
    
    # Get file size
    db_size = db_path.stat().st_size
    db_size_mb = db_size / (1024 * 1024)
    
    print()
    print("=" * 100)
    print("✅ PURGE COMPLETE")
    print("=" * 100)
    print()
    print(f"Deleted:")
    print(f"  Files: {file_count:,}")
    print(f"  Scans: {scan_count:,}")
    print()
    print(f"Remaining:")
    print(f"  Files: {remaining_files:,}")
    print(f"  Scans: {remaining_scans:,}")
    print()
    print(f"Database size: {db_size_mb:.2f} MB")
    print()
    print("The database is now empty and ready for a fresh scan.")
    print()

def main():
    parser = argparse.ArgumentParser(
        description="Purge all file and scan data from the database"
    )
    parser.add_argument(
        "--db",
        default="data/projects.db",
        help="Path to database (default: data/projects.db)"
    )
    
    args = parser.parse_args()
    db_path = Path(args.db)
    
    if not db_path.exists():
        print(f"❌ Error: Database not found: {db_path}")
        return
    
    purge_database(db_path)

if __name__ == "__main__":
    main()
