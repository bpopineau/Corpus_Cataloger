# Corpus Cataloger

Quick start:

1) `pip install -r requirements.txt`
2) Copy `config/catalog.yaml.example` to `config/catalog.yaml` and set your roots.
3) Run a small test:
   ```powershell
   python -m catalog.scan --config config/catalog.yaml --max-workers 4
   ```
4) Export Parquet:
   ```powershell
   python -m catalog.export --db data/projects.db --out data/parquet
   ```
