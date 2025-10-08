#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to analyze file extensions and suggest candidates for deletion.
"""

import sqlite3
from pathlib import Path

db_path = Path("data/projects.db")

print("File Extension Analysis")
print("=" * 80)

if not db_path.exists():
    print(f"ERROR: Database not found: {db_path}")
    exit(1)

with sqlite3.connect(str(db_path)) as con:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    
    # Get total count
    cur.execute("SELECT COUNT(*) FROM files")
    total_files = cur.fetchone()[0]
    print(f"Total files: {total_files:,}\n")
    
    # Get all extensions with counts and sample files
    print("All File Extensions (with counts and samples):")
    print("-" * 80)
    
    cur.execute("""
        SELECT ext, COUNT(*) as count 
        FROM files 
        WHERE ext IS NOT NULL 
        GROUP BY ext 
        ORDER BY count DESC
    """)
    
    extensions_data = []
    
    for row in cur.fetchall():
        ext = row[0]
        count = row[1]
        percentage = (count / total_files * 100)
        
        # Get sample file names
        cur.execute("""
            SELECT name 
            FROM files 
            WHERE ext = ? 
            LIMIT 3
        """, (ext,))
        samples = [r[0] for r in cur.fetchall()]
        
        extensions_data.append({
            'ext': ext,
            'count': count,
            'percentage': percentage,
            'samples': samples
        })
    
    # Display all extensions
    for data in extensions_data:
        print(f"\n{data['ext']}: {data['count']:,} files ({data['percentage']:.2f}%)")
        print(f"  Samples:")
        for sample in data['samples']:
            print(f"    - {sample}")
    
    # Categorize extensions
    print("\n" + "=" * 80)
    print("CATEGORIZED ANALYSIS")
    print("=" * 80)
    
    # Define categories
    categories = {
        'Documents (Keep)': {
            'exts': ['.pdf', '.docx', '.doc', '.xlsx', '.xls', '.pptx', '.ppt', '.txt', '.rtf', '.odt'],
            'description': 'Standard office documents - typically important'
        },
        'CAD/Technical (Keep)': {
            'exts': ['.dwg', '.dxf', '.skp', '.rvt', '.rfa', '.ifc', '.stp', '.step'],
            'description': 'CAD and technical drawings - typically important'
        },
        'Email (Consider)': {
            'exts': ['.msg', '.eml', '.pst', '.ost'],
            'description': 'Email files - may be archival, consider if needed'
        },
        'Temporary/Cache (Delete)': {
            'exts': ['.tmp', '.temp', '.cache', '.bak', '.old', '~', '.crdownload', '.part'],
            'description': 'Temporary files - usually safe to delete'
        },
        'System/Hidden (Delete)': {
            'exts': ['.db', '.ini', '.dat', '.log', '.lock', '.dll', '.sys'],
            'description': 'System files - often not needed for document management'
        },
        'Backup/Archive (Consider)': {
            'exts': ['.bak', '.backup', '.old', '.orig'],
            'description': 'Backup versions - may be redundant'
        },
        'Compressed (Keep/Consider)': {
            'exts': ['.zip', '.rar', '.7z', '.tar', '.gz'],
            'description': 'Archives - may contain important files'
        }
    }
    
    for category, info in categories.items():
        matching = [d for d in extensions_data if d['ext'].lower() in info['exts']]
        if matching:
            print(f"\n{category}:")
            print(f"  {info['description']}")
            total_in_category = sum(d['count'] for d in matching)
            print(f"  Total: {total_in_category:,} files")
            for d in matching:
                print(f"    {d['ext']}: {d['count']:,} ({d['percentage']:.2f}%)")
    
    # Find extensions not in any category
    all_categorized = []
    for cat_info in categories.values():
        all_categorized.extend(cat_info['exts'])
    
    uncategorized = [d for d in extensions_data if d['ext'].lower() not in all_categorized]
    
    if uncategorized:
        print(f"\nOther/Uncategorized Extensions:")
        for d in uncategorized:
            print(f"  {d['ext']}: {d['count']:,} ({d['percentage']:.2f}%)")
            print(f"    Samples: {', '.join(d['samples'][:2])}")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    
    print("""
Based on the analysis, here are recommendations for deletion:

1. SAFE TO DELETE (if present):
   - Temporary files (.tmp, .temp, .bak, .old)
   - System files (.db, .ini, .log, .dat)
   - Backup versions (.backup, .orig)
   - Cache files (.cache)
   
2. CONSIDER DELETING:
   - Email files (.msg) - Only if you don't need email archives
     Current: Check if these are important communications
   
3. KEEP:
   - Documents (.pdf, .docx, .xlsx) - Primary content
   - CAD files (.dwg) - Technical drawings
   
4. INVESTIGATE:
   - Any unusual extensions with high counts
   - Files without extensions (NULL)
   - Very small files that might be system generated
""")
    
    # Check for files without extensions
    cur.execute("SELECT COUNT(*) FROM files WHERE ext IS NULL OR ext = ''")
    no_ext_count = cur.fetchone()[0]
    if no_ext_count > 0:
        print(f"\nFiles without extension: {no_ext_count:,}")
        cur.execute("SELECT name FROM files WHERE ext IS NULL OR ext = '' LIMIT 5")
        print("  Samples:")
        for row in cur.fetchall():
            print(f"    - {row[0]}")

print("\n" + "=" * 80)
print("Analysis complete!")
