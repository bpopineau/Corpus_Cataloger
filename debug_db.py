#!/usr/bin/env python3
"""Debug script to test database connectivity and data retrieval independently of GUI."""

import sqlite3
import sys
from pathlib import Path

def test_database_connectivity():
    """Test basic database operations."""
    db_path = Path("data/projects.db")
    
    print(f"=== Database Debug Test ===")
    print(f"Database path: {db_path}")
    print(f"Database exists: {db_path.exists()}")
    
    if not db_path.exists():
        print("❌ Database file not found!")
        return False
        
    print(f"Database size: {db_path.stat().st_size:,} bytes")
    
    try:
        # Test connection
        with sqlite3.connect(str(db_path)) as con:
            print("✅ Database connection successful")
            
            # Test basic query
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cur.fetchall()]
            print(f"📋 Tables found: {tables}")
            
            if 'files' not in tables:
                print("❌ 'files' table not found!")
                return False
                
            # Test row count
            cur.execute("SELECT COUNT(*) FROM files")
            total_rows = cur.fetchone()[0]
            print(f"📊 Total rows in files table: {total_rows:,}")
            
            if total_rows == 0:
                print("❌ No data in files table!")
                return False
                
            # Test sample data fetch (like GUI does)
            print("\n=== Testing GUI-style data fetch ===")
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT file_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc, state, error_msg FROM files LIMIT 5")
            sample_rows = cur.fetchall()
            
            print(f"✅ Fetched {len(sample_rows)} sample rows")
            for i, row in enumerate(sample_rows):
                print(f"  Row {i+1}: {dict(row)}")
                
            # Test the exact query used by GUI
            print("\n=== Testing exact GUI query ===")
            cur.execute("SELECT file_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc, state, error_msg FROM files")
            all_rows = cur.fetchall()
            print(f"✅ GUI query fetched {len(all_rows):,} rows")
            
            # Test row conversion (like GUI does)
            print("\n=== Testing row conversion ===")
            sample_row = all_rows[0] if all_rows else None
            if sample_row:
                # Test direct access
                print(f"Direct access - name: {sample_row['name']}")
                print(f"Direct access - path: {sample_row['path_abs']}")
                
                # Test dict conversion
                row_dict = dict(zip(sample_row.keys(), sample_row))
                print(f"Dict conversion: {row_dict}")
                
            return True
            
    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_gui_row_helpers():
    """Test the row helper functions used by GUI."""
    print(f"\n=== Testing GUI Row Helpers ===")
    
    # Import the helpers from gui module
    sys.path.insert(0, '.')
    try:
        from catalog.gui import row_get, row_as_dict
        
        # Create a mock sqlite3.Row-like object
        class MockRow:
            def __init__(self, data):
                self._data = data
                
            def keys(self):
                return self._data.keys()
                
            def __getitem__(self, key):
                return self._data[key]
                
            def __iter__(self):
                return iter(self._data.values())
        
        # Test data
        test_data = {
            'name': 'test.txt',
            'path_abs': '/path/to/test.txt',
            'size_bytes': 1024,
            'state': 'done'
        }
        
        mock_row = MockRow(test_data)
        
        # Test row_get
        print(f"row_get(mock_row, 'name'): {row_get(mock_row, 'name')}")
        print(f"row_get(mock_row, 'nonexistent', 'default'): {row_get(mock_row, 'nonexistent', 'default')}")
        
        # Test row_as_dict
        converted = row_as_dict(mock_row)
        print(f"row_as_dict result: {converted}")
        print(f"Conversion successful: {converted == test_data}")
        
        return True
        
    except Exception as e:
        print(f"❌ GUI helper error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 Starting comprehensive database debug...")
    
    success = True
    success &= test_database_connectivity()
    success &= test_gui_row_helpers()
    
    if success:
        print("\n✅ All database tests passed! The issue is likely in the GUI code.")
    else:
        print("\n❌ Database tests failed! Fix database issues first.")
        
    print("\n=== Next Steps ===")
    if success:
        print("1. The database is working fine")
        print("2. Run the GUI with debug logging to see where data is lost")
        print("3. Check if FileExplorerWidget.ensure_loaded() is being called")
        print("4. Verify the table model is actually receiving the data")
    else:
        print("1. Fix the database connectivity issues shown above")
        print("2. Ensure the database file exists and has data")
        print("3. Re-run this script to verify fixes")