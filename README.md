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

5) Detect duplicates (fast path):
   ```powershell
   python -m catalog.dedupe --config config/catalog.yaml --max-workers 16
   ```

   Duplicate detection automatically skips any files that no longer exist in the
   database (for example, after running cleanup scripts) and only hashes files that
   share a size with at least one other entry. You can tune performance in
   `config/catalog.yaml` under the `dedupe` section:

   | Setting                | Purpose                                               |
   | ---------------------- | ----------------------------------------------------- |
   | `max_workers`          | Number of threads reading files concurrently          |
   | `small_file_threshold` | Files smaller than this go straight to full SHA-256   |
   | `quick_hash_bytes`     | Bytes sampled from file head/tail for the quick hash  |
   | `sha_chunk_bytes`      | Streaming chunk size for the full SHA-256 computation |

   For network file shares, increasing `max_workers`, `quick_hash_bytes`, and
   `sha_chunk_bytes` can dramatically reduce wall-clock time by keeping more
   I/O requests in flight.

## Development

For static type checking with mypy or editor integrations, the project ships the `types-PyYAML` stub package alongside runtime dependencies. If you installed requirements before this addition, re-run the install step to pick up the updated stubs:

```powershell
pip install -r requirements.txt
```
