from catalog.db import connect
from pathlib import Path

con = connect(Path('data/projects.db'))
cur = con.cursor()

cur.execute('SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL')
print(f'Files with SHA256: {cur.fetchone()[0]:,}')

cur.execute('SELECT COUNT(*) FROM files WHERE state = "done"')
print(f'Files marked done: {cur.fetchone()[0]:,}')

cur.execute('SELECT COUNT(*) FROM files WHERE state IN ("quick_hashed", "sha_verified")')
print(f'Files in progress: {cur.fetchone()[0]:,}')

cur.execute('SELECT state, COUNT(*) FROM files GROUP BY state ORDER BY COUNT(*) DESC')
print('\nFile states:')
for row in cur.fetchall():
    print(f'  {row[0] or "(null)"}: {row[1]:,}')

cur.execute('''
    SELECT COUNT(*) as dup_count, COUNT(DISTINCT sha256) as unique_hashes
    FROM (
        SELECT sha256
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
    )
''')
row = cur.fetchone()
print(f'\nDuplicate analysis:')
print(f'  Files with duplicate SHA256: {row[0]:,}')
print(f'  Unique duplicate hashes: {row[1]:,}')

con.close()
