from __future__ import annotations
import os, sqlite3, subprocess, sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence
from functools import partial
from concurrent.futures import Future, ThreadPoolExecutor
from PySide6 import QtWidgets, QtCore, QtGui
from PySide6.QtCore import Qt, QModelIndex, QPersistentModelIndex

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
            "[RUN] Effective scanner settings: max_workers=%s, chunk_bytes=%s"
            % (cfg.scanner.max_workers, cfg.scanner.io_chunk_bytes)
        )
        return cfg


DISPLAY_ROLE = int(Qt.ItemDataRole.DisplayRole)
USER_ROLE = int(Qt.ItemDataRole.UserRole)
TOOLTIP_ROLE = int(Qt.ItemDataRole.ToolTipRole)

NO_ITEM_FLAGS = Qt.ItemFlag.NoItemFlags
ENABLED_ITEM_FLAG = Qt.ItemFlag.ItemIsEnabled
SELECTABLE_ITEM_FLAG = Qt.ItemFlag.ItemIsSelectable
CASE_INSENSITIVE = Qt.CaseSensitivity.CaseInsensitive
SPLIT_HORIZONTAL = Qt.Orientation.Horizontal

FILE_PATH_ROLE = USER_ROLE + 1
IS_DIRECTORY_ROLE = USER_ROLE + 2
ROW_DATA_ROLE = USER_ROLE + 3


def format_bytes(size: Optional[int]) -> str:
    if size is None:
        return ""
    if size < 1024:
        return f"{size} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f} {unit}"
    return f"{value:.1f} EB"


def row_get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    if row is None:
        return default
    getter = getattr(row, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            pass
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        keys_method = getattr(row, "keys", None)
        if callable(keys_method):
            keys = keys_method()
            if key in keys:
                try:
                    return row[key]  # type: ignore[index]
                except Exception:
                    pass
    return default


def row_as_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    result: Dict[str, Any] = {}
    keys_method = getattr(row, "keys", None)
    if callable(keys_method):
        for key in keys_method():
            result[key] = row_get(row, key)
    return result


class FileTableModel(QtCore.QAbstractTableModel):
    COLUMNS: List[tuple[str, str]] = [
        ("name", "Name"),
        ("ext", "Ext"),
        ("dir", "Directory"),
        ("size_bytes", "Size"),
        ("mtime_utc", "Modified"),
        ("ctime_utc", "Created"),
        ("state", "State"),
        ("error_msg", "Error"),
    ]

    def __init__(self, parent: Optional[QtCore.QObject] = None) -> None:
        super().__init__(parent)
        self._rows: List[Any] = []

    def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
        return len(self.COLUMNS)

    def data(self, index: QModelIndex | QPersistentModelIndex, role: int = DISPLAY_ROLE) -> Any:
        if not index.isValid():
            return None
        row = self._rows[index.row()]
        key = self.COLUMNS[index.column()][0]
        value = row_get(row, key)
        if role == DISPLAY_ROLE:
            if key == "size_bytes":
                return format_bytes(value)
            if value is None:
                return ""
            return value
        if role == USER_ROLE:
            if key == "size_bytes":
                return value or 0
            return value or ""
        if role == TOOLTIP_ROLE:
            if key in {"name", "dir"}:
                return row_get(row, "path_abs")
            if key == "error_msg" and value:
                return value
        if role == ROW_DATA_ROLE:
            return row_as_dict(row)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = DISPLAY_ROLE) -> Any:
        if orientation == Qt.Orientation.Horizontal and role == DISPLAY_ROLE:
            return self.COLUMNS[section][1]
        return super().headerData(section, orientation, role)

    def flags(self, index: QModelIndex | QPersistentModelIndex):
        if not index.isValid():
            return NO_ITEM_FLAGS
        return ENABLED_ITEM_FLAG | SELECTABLE_ITEM_FLAG

    def set_rows(self, rows: Sequence[Any]) -> None:
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def row_data(self, row: int) -> Dict[str, Any]:
        if 0 <= row < len(self._rows):
            return row_as_dict(self._rows[row])
        return {}

    def raw_row(self, row: int) -> Any:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return {}


class FileFilterProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent: Optional[QtCore.QObject] = None) -> None:
        super().__init__(parent)
        self._filter_text: str = ""
        self._state_filter: str = "All"
        self._row_accessor: Optional[Callable[[int], Dict[str, Any]]] = None

    def setRowAccessor(self, accessor: Callable[[int], Dict[str, Any]]) -> None:
        self._row_accessor = accessor

    def setFilterText(self, text: str) -> None:
        self._filter_text = (text or "").strip().lower()
        self.invalidateFilter()

    def setStateFilter(self, state: str) -> None:
        self._state_filter = state or "All"
        self.invalidateFilter()

    def matches(self, row: Dict[str, Any]) -> bool:
        return self._accept_row(row)

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex | QPersistentModelIndex) -> bool:
        row: Dict[str, Any]
        if self._row_accessor:
            row = self._row_accessor(source_row)
        else:
            model = self.sourceModel()
            if hasattr(model, "row_data"):
                row = model.row_data(source_row)
            else:
                row = {}
        return self._accept_row(row)

    def _accept_row(self, row: Dict[str, Any]) -> bool:
        if not row:
            return False
        if self._state_filter != "All":
            if (row_get(row, "state") or "") != self._state_filter:
                return False
        if self._filter_text:
            haystack = " ".join(
                part for part in [
                    row_get(row, "path_abs"),
                    row_get(row, "name"),
                    row_get(row, "ext"),
                    row_get(row, "state"),
                    row_get(row, "error_msg"),
                ]
                if part
            ).lower()
            if self._filter_text not in haystack:
                return False
        return True


class FileExplorerWidget(QtWidgets.QWidget):
    def __init__(self, db_path_provider: Callable[[], Path], parent: Optional[QtWidgets.QWidget] = None) -> None:
        super().__init__(parent)
        self._db_path_provider = db_path_provider
        self._cached_db_path: Optional[Path] = None
        self._rows: List[Any] = []
        self._needs_reload = True
        self._executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=1)
        self._current_future: Optional[Future] = None
        self._loading = False
        self._pending_reload = False
        self._requested_db_path: Optional[Path] = None
        self._active_db_path: Optional[Path] = None
        self._tree_dirty = True
        self._tree_row_limit = 50000

        layout = QtWidgets.QVBoxLayout(self)

        toolbar = QtWidgets.QHBoxLayout()
        self.filterEdit = QtWidgets.QLineEdit()
        self.filterEdit.setPlaceholderText("Filter by name, path, extension, or state...")
        self.stateCombo = QtWidgets.QComboBox()
        self.stateCombo.addItem("All")
        self.viewToggle = QtWidgets.QButtonGroup(self)
        self.tableBtn = QtWidgets.QRadioButton("Table view")
        self.treeBtn = QtWidgets.QRadioButton("Tree view")
        self.tableBtn.setChecked(True)
        self.viewToggle.addButton(self.tableBtn, 0)
        self.viewToggle.addButton(self.treeBtn, 1)
        self.refreshBtn = QtWidgets.QPushButton("Reload")

        toolbar.addWidget(QtWidgets.QLabel("Filter:"))
        toolbar.addWidget(self.filterEdit, 1)
        toolbar.addWidget(QtWidgets.QLabel("State:"))
        toolbar.addWidget(self.stateCombo)
        toolbar.addSpacing(12)
        toolbar.addWidget(self.tableBtn)
        toolbar.addWidget(self.treeBtn)
        toolbar.addSpacing(12)
        toolbar.addWidget(self.refreshBtn)
        layout.addLayout(toolbar)

        self.stack = QtWidgets.QStackedWidget()
        layout.addWidget(self.stack, 1)

        # Table view setup
        self.tableModel = FileTableModel(self)
        self.proxyModel = FileFilterProxyModel(self)
        self.proxyModel.setSourceModel(self.tableModel)
        self.proxyModel.setRowAccessor(self.tableModel.raw_row)
        self.proxyModel.setFilterCaseSensitivity(CASE_INSENSITIVE)
        self.proxyModel.setSortCaseSensitivity(CASE_INSENSITIVE)
        self.proxyModel.setSortRole(USER_ROLE)

        self.tableView = QtWidgets.QTableView()
        self.tableView.setModel(self.proxyModel)
        self.tableView.setSortingEnabled(True)
        self.tableView.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.tableView.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.tableView.customContextMenuRequested.connect(self._show_table_context_menu)
        header = self.tableView.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.tableView.doubleClicked.connect(self._handle_table_double_click)
        self.stack.addWidget(self.tableView)

        # Tree view setup
        self.treeModel = QtGui.QStandardItemModel()
        self.treeModel.setHorizontalHeaderLabels(["Name", "Size", "Ext", "State"])
        self.treeView = QtWidgets.QTreeView()
        self.treeView.setModel(self.treeModel)
        self.treeView.setSortingEnabled(True)
        self.treeView.doubleClicked.connect(self._handle_tree_double_click)
        self.treeView.header().setStretchLastSection(True)
        self.treeView.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.treeView.customContextMenuRequested.connect(self._show_tree_context_menu)
        self.stack.addWidget(self.treeView)

        self.statusLabel = QtWidgets.QLabel("Ready")
        layout.addWidget(self.statusLabel)

        self.filterEdit.textChanged.connect(self._on_filter_text)
        self.stateCombo.currentTextChanged.connect(self._on_state_change)
        self.viewToggle.idToggled.connect(self._on_view_toggled)
        self.refreshBtn.clicked.connect(self.refresh_data)
        self.destroyed.connect(lambda *_: self.shutdown())

    def mark_stale(self) -> None:
        self._needs_reload = True

    def ensure_loaded(self) -> None:
        db_path = self._db_path_provider()
        if self._needs_reload or self._cached_db_path != db_path:
            self.refresh_data()

    def refresh_data(self) -> None:
        db_path = self._db_path_provider()
        self._requested_db_path = db_path
        if not db_path.exists():
            self._rows = []
            self._update_models([])
            self.statusLabel.setText(f"Database not found: {db_path}")
            self._needs_reload = True
            self._cached_db_path = None
            return
        if self._loading:
            if self._active_db_path is not None and self._active_db_path != db_path:
                self._pending_reload = True
            return
        self._start_load(db_path)

    def _start_load(self, db_path: Path) -> None:
        if self._executor is None:
            return
        self._loading = True
        self._pending_reload = False
        self._needs_reload = False
        self._active_db_path = Path(db_path)
        self.statusLabel.setText(f"Loading files from {db_path}...")
        future = self._executor.submit(self._fetch_rows, Path(db_path))
        self._current_future = future
        future.add_done_callback(partial(self._on_future_done, Path(db_path)))

    @staticmethod
    def _fetch_rows(db_path: Path) -> List[Any]:
        with sqlite3.connect(str(db_path)) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                """SELECT file_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc, state, error_msg FROM files"""
            )
            return cur.fetchall()

    def _on_future_done(self, path: Path, future: Future) -> None:
        try:
            rows = future.result()
        except Exception as exc:
            QtCore.QTimer.singleShot(0, lambda: self._handle_future_failure(path, str(exc)))
        else:
            QtCore.QTimer.singleShot(0, lambda: self._handle_future_success(path, rows))

    def _handle_future_success(self, path: Path, rows: Sequence[Any]) -> None:
        self._current_future = None
        self._rows = list(rows)
        self._cached_db_path = path
        self._requested_db_path = path
        self._update_state_options(self._rows)
        self._update_models(self._rows)
        self.statusLabel.setText(f"Loaded {len(self._rows)} files from {path}")
        self._finish_loading()

    def _handle_future_failure(self, path: Path, error: str) -> None:
        self._current_future = None
        self._rows = []
        self._update_models([])
        self.statusLabel.setText(f"Error loading {path}: {error}")
        self._needs_reload = True
        self._cached_db_path = None
        self._finish_loading()

    def _finish_loading(self) -> None:
        self._loading = False
        self._active_db_path = None
        if self._pending_reload:
            self._pending_reload = False
            QtCore.QTimer.singleShot(0, self.refresh_data)

    def shutdown(self) -> None:
        self._pending_reload = False
        future = self._current_future
        if future and not future.done():
            future.cancel()
        self._current_future = None
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        self._loading = False
        self._active_db_path = None

    def _update_state_options(self, rows: Sequence[Any]) -> None:
        states = sorted({str(state) for state in (row_get(row, "state") for row in rows) if state})
        current = self.stateCombo.currentText()
        self.stateCombo.blockSignals(True)
        self.stateCombo.clear()
        self.stateCombo.addItem("All")
        for state in states:
            self.stateCombo.addItem(state)
        if current and self.stateCombo.findText(current) >= 0:
            self.stateCombo.setCurrentText(current)
        self.stateCombo.blockSignals(False)

    def _update_models(self, rows: Sequence[Any]) -> None:
        self.tableModel.set_rows(rows)
        self.proxyModel.invalidateFilter()
        self._tree_dirty = True
        self._maybe_rebuild_tree()

    def _rebuild_tree(self, rows: Sequence[Any]) -> None:
        self.treeModel.removeRows(0, self.treeModel.rowCount())
        root = self.treeModel.invisibleRootItem()
        nodes: Dict[str, QtGui.QStandardItem] = {}
        parts_cache: Dict[str, Sequence[str]] = {}

        def ensure_directory(path_str: str) -> QtGui.QStandardItem:
            if not path_str:
                return root
            existing = nodes.get(path_str)
            if existing is not None:
                return existing
            parts = parts_cache.get(path_str)
            if parts is None:
                parts_cache[path_str] = parts = Path(path_str).parts
            parent = root
            current_path = ""
            for part in parts:
                current_path = part if not current_path else os.path.join(current_path, part)
                node = nodes.get(current_path)
                if node is None:
                    items = self._create_dir_items(part, current_path)
                    parent.appendRow(items)
                    node = items[0]
                    nodes[current_path] = node
                parent = node
            nodes[path_str] = parent
            return parent

        truncated = False
        limit = self._tree_row_limit

        for idx, row in enumerate(rows):
            if limit and idx >= limit:
                truncated = True
                break
            dir_path = row_get(row, "dir", "")
            parent_item = ensure_directory(dir_path if isinstance(dir_path, str) else "")
            file_items = self._create_file_items(row)
            parent_item.appendRow(file_items)

        self.treeView.expandToDepth(0)
        if truncated:
            self.treeView.setToolTip(
                f"Tree view truncated to first {limit:,} entries (of {len(rows):,}). Apply filters to narrow results."
            )
        else:
            self.treeView.setToolTip("")

    def _maybe_rebuild_tree(self) -> None:
        if self.stack.currentWidget() is not self.treeView:
            return
        if not self._tree_dirty:
            return
        filtered_rows = [row for row in self._rows if self.proxyModel.matches(row)]
        self._rebuild_tree(filtered_rows)
        self._tree_dirty = False

    def _create_dir_items(self, name: str, full_path: str) -> List[QtGui.QStandardItem]:
        name_item = QtGui.QStandardItem(name)
        name_item.setEditable(False)
        name_item.setData(full_path, FILE_PATH_ROLE)
        name_item.setData(True, IS_DIRECTORY_ROLE)
        size_item = QtGui.QStandardItem("")
        size_item.setEditable(False)
        ext_item = QtGui.QStandardItem("")
        ext_item.setEditable(False)
        state_item = QtGui.QStandardItem("")
        state_item.setEditable(False)
        return [name_item, size_item, ext_item, state_item]

    def _create_file_items(self, row: Any) -> List[QtGui.QStandardItem]:
        name_item = QtGui.QStandardItem(row_get(row, "name") or "")
        name_item.setEditable(False)
        name_item.setData(row_get(row, "path_abs"), FILE_PATH_ROLE)
        name_item.setData(False, IS_DIRECTORY_ROLE)
        name_item.setData(row, ROW_DATA_ROLE)

        size_item = QtGui.QStandardItem(format_bytes(row_get(row, "size_bytes")))
        size_item.setEditable(False)
        size_item.setData(row_get(row, "size_bytes") or 0, USER_ROLE)

        ext_item = QtGui.QStandardItem(row_get(row, "ext") or "")
        ext_item.setEditable(False)

        state_item = QtGui.QStandardItem(row_get(row, "state") or "")
        state_item.setEditable(False)

        return [name_item, size_item, ext_item, state_item]

    def _on_filter_text(self, text: str) -> None:
        self.proxyModel.setFilterText(text)
        self._tree_dirty = True
        self._maybe_rebuild_tree()

    def _on_state_change(self, state: str) -> None:
        self.proxyModel.setStateFilter(state)
        self._tree_dirty = True
        self._maybe_rebuild_tree()

    def _on_view_toggled(self, button_id: int, checked: bool) -> None:
        if not checked:
            return
        self.stack.setCurrentIndex(button_id)
        self._maybe_rebuild_tree()

    def _handle_table_double_click(self, index: QtCore.QModelIndex) -> None:
        if not index.isValid():
            return
        source_index = self.proxyModel.mapToSource(index)
        row = self.tableModel.raw_row(source_index.row())
        self._open_path(row_get(row, "path_abs"))

    def _handle_tree_double_click(self, index: QtCore.QModelIndex) -> None:
        if not index.isValid():
            return
        item = self.treeModel.itemFromIndex(index)
        if not item:
            return
        if item.data(IS_DIRECTORY_ROLE):
            if self.treeView.isExpanded(index):
                self.treeView.collapse(index)
            else:
                self.treeView.expand(index)
            return
        path = item.data(FILE_PATH_ROLE)
        self._open_path(path)

    def _show_table_context_menu(self, pos: QtCore.QPoint) -> None:
        index = self.tableView.indexAt(pos)
        if not index.isValid():
            return
        source_index = self.proxyModel.mapToSource(index)
        row = self.tableModel.raw_row(source_index.row())
        path = row_get(row, "path_abs")
        menu = QtWidgets.QMenu(self)
        open_action = menu.addAction("Open file")
        reveal_action = menu.addAction("Reveal in folder")
        copy_action = menu.addAction("Copy path")
        chosen = menu.exec(self.tableView.viewport().mapToGlobal(pos))
        if chosen == open_action:
            self._open_path(path)
        elif chosen == reveal_action:
            self._reveal_in_explorer(path)
        elif chosen == copy_action:
            self._copy_path(path)

    def _show_tree_context_menu(self, pos: QtCore.QPoint) -> None:
        index = self.treeView.indexAt(pos)
        if not index.isValid():
            return
        item = self.treeModel.itemFromIndex(index)
        if not item:
            return
        path = item.data(FILE_PATH_ROLE)
        if not path:
            return
        is_dir = bool(item.data(IS_DIRECTORY_ROLE))
        menu = QtWidgets.QMenu(self)
        if is_dir:
            open_action = menu.addAction("Open folder")
            copy_action = menu.addAction("Copy path")
            chosen = menu.exec(self.treeView.viewport().mapToGlobal(pos))
            if chosen == open_action:
                self._reveal_in_explorer(path)
            elif chosen == copy_action:
                self._copy_path(path)
        else:
            open_action = menu.addAction("Open file")
            reveal_action = menu.addAction("Reveal in folder")
            copy_action = menu.addAction("Copy path")
            chosen = menu.exec(self.treeView.viewport().mapToGlobal(pos))
            if chosen == open_action:
                self._open_path(path)
            elif chosen == reveal_action:
                self._reveal_in_explorer(path)
            elif chosen == copy_action:
                self._copy_path(path)

    def _open_path(self, path: Optional[str]) -> None:
        if not path:
            return
        p = Path(path)
        if not p.exists():
            QtWidgets.QMessageBox.warning(self, "File missing", f"File not found on disk:\n{path}")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(p))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(p)])
            else:
                subprocess.Popen(["xdg-open", str(p)])
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Open failed", f"Could not open file:\n{e}")

    def _reveal_in_explorer(self, path: Optional[str]) -> None:
        if not path:
            return
        p = Path(path)
        if not p.exists():
            QtWidgets.QMessageBox.warning(self, "Path missing", f"Path not found on disk:\n{path}")
            return
        try:
            if sys.platform.startswith("win"):
                if p.is_file():
                    subprocess.Popen(["explorer", "/select,", str(p)])
                else:
                    subprocess.Popen(["explorer", str(p)])
            elif sys.platform == "darwin":
                if p.is_file():
                    subprocess.Popen(["open", "-R", str(p)])
                else:
                    subprocess.Popen(["open", str(p)])
            else:
                target = p if p.is_dir() else p.parent
                subprocess.Popen(["xdg-open", str(target)])
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Open failed", f"Could not open location:\n{e}")

    def _copy_path(self, path: Optional[str]) -> None:
        if not path:
            return
        QtWidgets.QApplication.clipboard().setText(path)


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

        layout.addWidget(settings_box)

        self.scanStatus = QtWidgets.QLabel("Idle")
        layout.addWidget(self.scanStatus)

        self.scanProgress = QtWidgets.QProgressBar()
        self.scanProgress.setRange(0, 1)
        self.scanProgress.setValue(0)
        layout.addWidget(self.scanProgress)

        self.dbProgress = QtWidgets.QProgressBar()
        self.dbProgress.setRange(0, 0)

        self.dbStats = QtWidgets.QPlainTextEdit()
        self.dbStats.setReadOnly(True)

        self.logView = QtWidgets.QPlainTextEdit()
        self.logView.setReadOnly(True)

        leftPane = QtWidgets.QWidget()
        leftPane.setMinimumWidth(320)
        leftLayout = QtWidgets.QVBoxLayout(leftPane)
        leftLayout.addWidget(QtWidgets.QLabel("Database progress"))
        leftLayout.addWidget(self.dbProgress)
        leftLayout.addWidget(QtWidgets.QLabel("Database stats"))
        leftLayout.addWidget(self.dbStats, 1)
        leftLayout.addWidget(QtWidgets.QLabel("Scan log"))
        leftLayout.addWidget(self.logView, 2)

        self.fileExplorer = FileExplorerWidget(self._current_db_path, self)
        splitter = QtWidgets.QSplitter(SPLIT_HORIZONTAL)
        splitter.addWidget(leftPane)
        splitter.addWidget(self.fileExplorer)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)
        layout.addWidget(splitter, 1)

        self.refreshBtn.clicked.connect(self._refresh_all)
        self.browseBtn.clicked.connect(self._select_folder)
        self.scanBtn.clicked.connect(self._start_scan)
        self.dbEdit.editingFinished.connect(self._on_db_path_changed)

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
            except Exception:
                pass

        self._scan_thread: Optional[QtCore.QThread] = None
        self._scan_worker: Optional[ScanWorker] = None

        self.fileExplorer.mark_stale()
        self.refresh_stats()

    def refresh_stats(self) -> None:
        db_path = self._current_db_path()
        explorer = getattr(self, "fileExplorer", None)

        if not db_path.exists():
            self.dbStats.setPlainText("DB not found. Run a scan from the CLI or start a new scan.")
            self.dbProgress.setRange(0, 0)
            if explorer:
                explorer.mark_stale()
                explorer.ensure_loaded()
            return

        try:
            with sqlite3.connect(str(db_path)) as con:
                cur = con.cursor()
                cur.execute("SELECT COUNT(*) FROM files")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM files WHERE state='done'")
                done = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM files WHERE state IN ('pending','quick_hashed','sha_pending')")
                pending = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM files WHERE state='error'")
                err = cur.fetchone()[0]

            self.dbStats.setPlainText(f"Total: {total}\nDone: {done}\nPending: {pending}\nErrors: {err}")
            if total > 0:
                self.dbProgress.setRange(0, total)
                self.dbProgress.setValue(done)
            else:
                self.dbProgress.setRange(0, 0)
        except Exception as e:
            self.dbStats.setPlainText(f"Error: {e}")
            self.dbProgress.setRange(0, 0)
            if explorer:
                explorer.mark_stale()
                explorer.ensure_loaded()
        else:
            if explorer:
                explorer.ensure_loaded()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self.fileExplorer.shutdown()
        super().closeEvent(event)

    def _refresh_all(self) -> None:
        self.fileExplorer.mark_stale()
        self.refresh_stats()

    def _on_db_path_changed(self) -> None:
        self.fileExplorer.mark_stale()
        self.refresh_stats()

    def _current_db_path(self) -> Path:
        text = self.dbEdit.text().strip()
        if not text:
            return Path("data/projects.db")
        return Path(text)

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

        db_path = self._current_db_path()
        if not db_path.parent.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)

        self.fileExplorer.mark_stale()

        self.scanBtn.setEnabled(False)
        self.rootEdit.setEnabled(False)
        self.browseBtn.setEnabled(False)
        self.scanStatus.setText(f"Scanning {root_path}...")
        self.scanProgress.setRange(0, 0)
        self.scanProgress.setValue(0)
        self.logView.clear()
        self.logView.appendPlainText(f"[INFO] Starting scan for: {root_path}")
        self.logView.appendPlainText(
            "[INFO] Settings: workers=%s chunk=%sKB"
            % (
                self.workerSpin.value(),
                self.chunkSpin.value(),
            )
        )

        self.workerSpin.setEnabled(False)
        self.chunkSpin.setEnabled(False)

        self._scan_thread = QtCore.QThread(self)
        overrides = dict(
            max_workers=self.workerSpin.value(),
            io_chunk_bytes=self.chunkSpin.value() * 1024,
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
        self._refresh_all()

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
        self._refresh_all()
        QtWidgets.QMessageBox.information(self, "Scan complete", "Scanning finished successfully.")

    @QtCore.Slot(str)
    def _scan_failed(self, error: str) -> None:
        self.scanStatus.setText("Scan failed")
        self.scanProgress.setRange(0, 1)
        self.scanProgress.setValue(0)
        self.logView.appendPlainText(f"[ERROR] {error}")
        self._refresh_all()
        QtWidgets.QMessageBox.critical(self, "Scan failed", error)


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
