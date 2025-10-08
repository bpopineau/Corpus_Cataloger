from __future__ import annotations
import sys, sqlite3
from pathlib import Path
from PySide6 import QtWidgets, QtCore

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Corpus Cataloger")
        self.resize(900, 500)
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)

        controls = QtWidgets.QHBoxLayout()
        self.dbEdit = QtWidgets.QLineEdit("data/projects.db")
        self.refreshBtn = QtWidgets.QPushButton("Refresh")
        controls.addWidget(QtWidgets.QLabel("DB:"))
        controls.addWidget(self.dbEdit, 1)
        controls.addWidget(self.refreshBtn)
        layout.addLayout(controls)

        self.progress = QtWidgets.QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        layout.addWidget(self.progress)

        self.stats = QtWidgets.QPlainTextEdit()
        self.stats.setReadOnly(True)
        layout.addWidget(self.stats, 1)

        self.refreshBtn.clicked.connect(self.refresh_stats)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.refresh_stats)
        self.timer.start(1500)

    def refresh_stats(self):
        db_path = Path(self.dbEdit.text())
        if not db_path.exists():
            self.stats.setPlainText("DB not found. Run a scan from the CLI.")
            self.progress.setRange(0, 0)
            return
        try:
            con = sqlite3.connect(str(db_path))
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM files")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM files WHERE state='done'")
            done = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM files WHERE state IN ('pending','quick_hashed','sha_pending')")
            pending = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM files WHERE state='error'")
            err = cur.fetchone()[0]
            con.close()
            self.stats.setPlainText(f"Total: {total}\nDone: {done}\nPending: {pending}\nErrors: {err}")
            if total > 0:
                self.progress.setRange(0, 100)
                self.progress.setValue(int(done / total * 100))
            else:
                self.progress.setRange(0, 0)
        except Exception as e:
            self.stats.setPlainText(f"Error: {e}")
            self.progress.setRange(0, 0)

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
