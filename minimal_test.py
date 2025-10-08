#!/usr/bin/env python3
"""Minimal GUI test to isolate the data loading issue."""

import sys
import sqlite3
from pathlib import Path
from PySide6 import QtWidgets, QtCore
from catalog.gui import FileTableModel, row_as_dict

class MinimalTestWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Minimal Database Test")
        self.setGeometry(100, 100, 800, 600)
        
        # Create central widget and layout
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        
        # Add status label
        self.status = QtWidgets.QLabel("Ready")
        layout.addWidget(self.status)
        
        # Add test button
        self.testBtn = QtWidgets.QPushButton("Load Database")
        self.testBtn.clicked.connect(self.load_database)
        layout.addWidget(self.testBtn)
        
        # Create table view with model
        self.tableModel = FileTableModel()
        self.tableView = QtWidgets.QTableView()
        self.tableView.setModel(self.tableModel)
        layout.addWidget(self.tableView)
        
        print("🔍 DEBUG: MinimalTestWindow initialized")
        
    def load_database(self):
        """Load data directly from database."""
        print("🔍 DEBUG: load_database clicked")
        
        db_path = Path("data/projects.db")
        if not db_path.exists():
            self.status.setText(f"Database not found: {db_path}")
            print(f"🔍 DEBUG: Database not found: {db_path}")
            return
            
        try:
            print("🔍 DEBUG: Opening database connection...")
            with sqlite3.connect(str(db_path)) as con:
                con.row_factory = sqlite3.Row
                cur = con.cursor()
                
                # Test with just first 100 rows to start
                print("🔍 DEBUG: Executing query...")
                cur.execute("SELECT file_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc, state, error_msg FROM files LIMIT 100")
                rows = cur.fetchall()
                
                print(f"🔍 DEBUG: Fetched {len(rows)} rows")
                if rows:
                    print(f"🔍 DEBUG: First row: {dict(rows[0])}")
                
                print("🔍 DEBUG: Setting rows in table model...")
                self.tableModel.set_rows(rows)
                
                print(f"🔍 DEBUG: Table model now has {self.tableModel.rowCount()} rows")
                self.status.setText(f"Loaded {len(rows)} rows")
                
                # Force table view to update
                print("🔍 DEBUG: Forcing table view update...")
                self.tableView.resizeColumnsToContents()
                self.tableView.update()
                
        except Exception as e:
            error_msg = f"Error loading database: {e}"
            print(f"🔍 DEBUG: {error_msg}")
            import traceback
            traceback.print_exc()
            self.status.setText(error_msg)

def main():
    print("🔍 DEBUG: Starting minimal test application...")
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication(sys.argv)
    
    window = MinimalTestWindow()
    window.show()
    
    print("🔍 DEBUG: Window shown, starting event loop...")
    
    # Auto-load data on startup
    QtCore.QTimer.singleShot(1000, window.load_database)
    
    return app.exec()

if __name__ == "__main__":
    sys.exit(main())