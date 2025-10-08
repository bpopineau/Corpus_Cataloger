from __future__ import annotations
import sys, sqlite3
from pathlib import Path
from typing import Dict, Optional
from PySide6 import QtWidgets, QtCore

from .config import CatalogConfig, ScannerConfig, load_config
from .scan import scan_root


class ScanWorker(QtCore.QObject):
    progress = QtCore.Signal(str, int, int, str)
    log = QtCore.Signal(str)
    finished = QtCore.Signal()
    failed = QtCore.Signal(str)

    def __init__(self, root_path: Path, db_path: Path, config_path: Optional[Path] = None, overrides: Optional[Dict[str, int]] = None, parent: Optional[QtCore.QObject] = None):
        super().__init__(parent)
        self._root = Path(root_path)
        self._db_path = Path(db_path)
        self._config_path = config_path
        self._overrides = overrides or {}

    @QtCore.Slot()
    def run(self) -> None:
        try:
            cfg = self._load_config()

            def on_progress(stage: str, current: int, total: int, message: str) -> None:
                self.progress.emit(stage, current, total, message)

            def on_log(message: str) -> None:
                self.log.emit(message)

            scan_root(str(self._root), cfg, progress_cb=on_progress, log_cb=on_log)
            self.finished.emit()
        except Exception as e:
            self.failed.emit(str(e))

    def _load_config(self) -> CatalogConfig:
        cfg: CatalogConfig
        if self._config_path and self._config_path.exists():
            cfg = load_config(self._config_path)
        else:
            cfg = CatalogConfig(roots=[str(self._root)])
        cfg.roots = [str(self._root)]
        cfg.db.path = str(self._db_path)
        for key, value in self._overrides.items():
            if value is None:
                continue
            if hasattr(cfg.scanner, key):
                setattr(cfg.scanner, key, value)
        self.log.emit(
            "[RUN] Effective scanner settings: max_workers=%s, chunk_bytes=%s, pdf_pages=%s"
            % (cfg.scanner.max_workers, cfg.scanner.io_chunk_bytes, cfg.scanner.probe_pdf_pages)
        )
        return cfg


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Corpus Cataloger")
        self.resize(900, 600)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        self._scanner_defaults = ScannerConfig()

        controls = QtWidgets.QHBoxLayout()
        self.dbEdit = QtWidgets.QLineEdit("data/projects.db")
        self.refreshBtn = QtWidgets.QPushButton("Refresh")
        controls.addWidget(QtWidgets.QLabel("DB:"))
        controls.addWidget(self.dbEdit, 1)
        controls.addWidget(self.refreshBtn)
        layout.addLayout(controls)

        scan_controls = QtWidgets.QHBoxLayout()
        self.rootEdit = QtWidgets.QLineEdit()
        self.rootEdit.setPlaceholderText("Select a folder to scan...")
        self.browseBtn = QtWidgets.QPushButton("Browse")
        self.scanBtn = QtWidgets.QPushButton("Start Scan")
        scan_controls.addWidget(QtWidgets.QLabel("Folder:"))
        scan_controls.addWidget(self.rootEdit, 1)
        scan_controls.addWidget(self.browseBtn)
        scan_controls.addWidget(self.scanBtn)
        layout.addLayout(scan_controls)

        settings_box = QtWidgets.QGroupBox("Scanner settings")
        settings_form = QtWidgets.QFormLayout(settings_box)

        self.workerSpin = QtWidgets.QSpinBox()
        self.workerSpin.setRange(1, 256)
        self.workerSpin.setValue(self._scanner_defaults.max_workers)
        self.workerSpin.setToolTip("Number of worker threads for hashing phase")
        settings_form.addRow("Workers", self.workerSpin)

        self.chunkSpin = QtWidgets.QSpinBox()
        self.chunkSpin.setRange(4, 65536)
        self.chunkSpin.setSingleStep(128)
        self.chunkSpin.setValue(max(4, self._scanner_defaults.io_chunk_bytes // 1024))
        self.chunkSpin.setSuffix(" KB")
        self.chunkSpin.setToolTip("Head/tail bytes read per file (KB)")
        settings_form.addRow("Chunk size", self.chunkSpin)

        self.pdfSpin = QtWidgets.QSpinBox()
        self.pdfSpin.setRange(1, 50)
        self.pdfSpin.setValue(self._scanner_defaults.probe_pdf_pages)
        self.pdfSpin.setToolTip("Pages inspected when probing PDF text")
        settings_form.addRow("PDF probe pages", self.pdfSpin)

        layout.addWidget(settings_box)

        self.scanStatus = QtWidgets.QLabel("Idle")
        layout.addWidget(self.scanStatus)

        self.scanProgress = QtWidgets.QProgressBar()
        self.scanProgress.setRange(0, 1)
        self.scanProgress.setValue(0)
        layout.addWidget(self.scanProgress)

        layout.addWidget(QtWidgets.QLabel("Database progress"))
        self.dbProgress = QtWidgets.QProgressBar()
        self.dbProgress.setRange(0, 0)
        layout.addWidget(self.dbProgress)

        layout.addWidget(QtWidgets.QLabel("Database stats"))
        self.dbStats = QtWidgets.QPlainTextEdit()
        self.dbStats.setReadOnly(True)
        layout.addWidget(self.dbStats, 1)

        layout.addWidget(QtWidgets.QLabel("Scan log"))
        self.logView = QtWidgets.QPlainTextEdit()
        self.logView.setReadOnly(True)
        layout.addWidget(self.logView, 1)

        self.refreshBtn.clicked.connect(self.refresh_stats)
        self.browseBtn.clicked.connect(self._select_folder)
        self.scanBtn.clicked.connect(self._start_scan)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.refresh_stats)
        self.timer.start(1500)

        self._config_path = Path("config/catalog.yaml")
        if self._config_path.exists():
            try:
                cfg = load_config(self._config_path)
                if cfg.db and cfg.db.path:
                    self.dbEdit.setText(cfg.db.path)
                if cfg.roots:
                    self.rootEdit.setText(cfg.roots[0])
                self.workerSpin.setValue(cfg.scanner.max_workers)
                self.chunkSpin.setValue(max(4, cfg.scanner.io_chunk_bytes // 1024))
                self.pdfSpin.setValue(cfg.scanner.probe_pdf_pages)
            except Exception:
                pass

        self._scan_thread: Optional[QtCore.QThread] = None
        self._scan_worker: Optional[ScanWorker] = None

        self.refresh_stats()

    def refresh_stats(self) -> None:
        db_path = Path(self.dbEdit.text())
        if not db_path.exists():
            self.dbStats.setPlainText("DB not found. Run a scan from the CLI or start a new scan.")
            self.dbProgress.setRange(0, 0)
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
            self.dbStats.setPlainText(f"Total: {total}\nDone: {done}\nPending: {pending}\nErrors: {err}")
            if total > 0:
                self.dbProgress.setRange(0, total)
                self.dbProgress.setValue(done)
            else:
                self.dbProgress.setRange(0, 0)
        except Exception as e:
            self.dbStats.setPlainText(f"Error: {e}")
            self.dbProgress.setRange(0, 0)

    def _select_folder(self) -> None:
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select folder to scan")
        if directory:
            self.rootEdit.setText(directory)

    def _start_scan(self) -> None:
        if self._scan_thread is not None:
            return

        root_text = self.rootEdit.text().strip()
        if not root_text:
            QtWidgets.QMessageBox.warning(self, "Select Folder", "Please choose a folder to scan.")
            return

        root_path = Path(root_text)
        if not root_path.exists():
            QtWidgets.QMessageBox.warning(self, "Invalid Folder", "The selected folder does not exist.")
            return

        db_path = Path(self.dbEdit.text().strip())
        if not db_path.parent.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)

        self.scanBtn.setEnabled(False)
        self.rootEdit.setEnabled(False)
        self.browseBtn.setEnabled(False)
        self.scanStatus.setText(f"Scanning {root_path}...")
        self.scanProgress.setRange(0, 0)
        self.scanProgress.setValue(0)
        self.logView.clear()
        self.logView.appendPlainText(f"[INFO] Starting scan for: {root_path}")
        self.logView.appendPlainText(
            "[INFO] Settings: workers=%s chunk=%sKB pdf_pages=%s"
            % (
                self.workerSpin.value(),
                self.chunkSpin.value(),
                self.pdfSpin.value(),
            )
        )

        self.workerSpin.setEnabled(False)
        self.chunkSpin.setEnabled(False)
        self.pdfSpin.setEnabled(False)

        self._scan_thread = QtCore.QThread(self)
        overrides = dict(
            max_workers=self.workerSpin.value(),
            io_chunk_bytes=self.chunkSpin.value() * 1024,
            probe_pdf_pages=self.pdfSpin.value(),
        )
        self._scan_worker = ScanWorker(
            root_path=root_path,
            db_path=db_path,
            config_path=self._config_path,
            overrides=overrides,
        )
        self._scan_worker.moveToThread(self._scan_thread)

        self._scan_thread.started.connect(self._scan_worker.run)
        self._scan_worker.progress.connect(self._handle_progress)
        self._scan_worker.log.connect(self._handle_log)
        self._scan_worker.finished.connect(self._scan_complete)
        self._scan_worker.failed.connect(self._scan_failed)

        self._scan_worker.finished.connect(self._scan_thread.quit)
        self._scan_worker.failed.connect(self._scan_thread.quit)
        self._scan_worker.finished.connect(self._scan_worker.deleteLater)
        self._scan_worker.failed.connect(self._scan_worker.deleteLater)
        self._scan_thread.finished.connect(self._clear_worker)
        self._scan_thread.start()

    @QtCore.Slot()
    def _clear_worker(self) -> None:
        if self._scan_thread:
            self._scan_thread.deleteLater()
        self._scan_thread = None
        self._scan_worker = None
        self.scanBtn.setEnabled(True)
        self.rootEdit.setEnabled(True)
        self.browseBtn.setEnabled(True)
        self.workerSpin.setEnabled(True)
        self.chunkSpin.setEnabled(True)
        self.pdfSpin.setEnabled(True)
        self.refresh_stats()

    @QtCore.Slot(str, int, int, str)
    def _handle_progress(self, stage: str, current: int, total: int, message: str) -> None:
        if total <= 0:
            self.scanProgress.setRange(0, 0)
        else:
            self.scanProgress.setRange(0, total)
            self.scanProgress.setValue(current)
        self.scanStatus.setText(message or stage.capitalize())

    @QtCore.Slot(str)
    def _handle_log(self, message: str) -> None:
        self.logView.appendPlainText(message)

    @QtCore.Slot()
    def _scan_complete(self) -> None:
        self.scanStatus.setText("Scan complete")
        self.scanProgress.setRange(0, 1)
        self.scanProgress.setValue(1)
        self.logView.appendPlainText("[DONE] Scan complete")
        QtWidgets.QMessageBox.information(self, "Scan complete", "Scanning finished successfully.")

    @QtCore.Slot(str)
    def _scan_failed(self, error: str) -> None:
        self.scanStatus.setText("Scan failed")
        self.scanProgress.setRange(0, 1)
        self.scanProgress.setValue(0)
        self.logView.appendPlainText(f"[ERROR] {error}")
        QtWidgets.QMessageBox.critical(self, "Scan failed", error)


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
