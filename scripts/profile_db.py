from __future__ import annotations
import sqlite3
import time
from pathlib import Path

def main() -> None:
    path = Path('data/projects.db')
    print('DB exists:', path.exists())
    if not path.exists():
        return
    con = sqlite3.connect(path)
    try:
        cur = con.cursor()
        t0 = time.perf_counter()
        cur.execute('SELECT COUNT(*) FROM files')
        total = cur.fetchone()[0]
        t1 = time.perf_counter()
        cur.execute('SELECT * FROM files LIMIT 1')
        cur.fetchone()
        t2 = time.perf_counter()
        cur.execute('SELECT file_id, scan_run_id, path_abs, dir, name, ext, size_bytes, mtime_utc, ctime_utc, state, error_code, error_msg FROM files')
        cur.fetchall()
        t3 = time.perf_counter()
        print(f'rows: {total}')
        print(f'count_time: {t1 - t0:.3f}s')
        print(f'fetch_single_time: {t2 - t1:.3f}s')
        print(f'fetch_all_time: {t3 - t2:.3f}s')
        print(f'total_time: {t3 - t0:.3f}s')
    finally:
        con.close()

if __name__ == '__main__':
    main()
