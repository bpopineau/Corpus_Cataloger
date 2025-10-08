from __future__ import annotations
from pathlib import Path
from typing import List
from pydantic import BaseModel, Field
import yaml

class ScannerConfig(BaseModel):
    max_workers: int = 8
    io_chunk_bytes: int = 65536

class DBConfig(BaseModel):
    path: str = "data/projects.db"
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"

class ExportConfig(BaseModel):
    parquet_dir: str = "data/parquet"
    schedule: str = "manual"

class CatalogConfig(BaseModel):
    roots: List[str]
    include_ext: List[str] = Field(default_factory=list)
    exclude_paths: List[str] = Field(default_factory=list)
    scanner: ScannerConfig = ScannerConfig()
    db: DBConfig = DBConfig()
    export: ExportConfig = ExportConfig()

def load_config(path: Path) -> CatalogConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return CatalogConfig(**data)
