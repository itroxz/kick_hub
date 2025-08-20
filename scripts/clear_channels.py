#!/usr/bin/env python3
"""
Helper to inspect and clear the `channels` table in local `fds_bot.db`.

Usage (PowerShell):
  cd C:/Users/Sam/Documents/programa/fds_bot
  py ./scripts/clear_channels.py show    # show first rows and schema
  py ./scripts/clear_channels.py clear   # set name and slug to empty strings
  py ./scripts/clear_channels.py drop    # DELETE all rows from channels

No backups are made. Use with caution.
"""
import sqlite3
import sys
from pathlib import Path

DB = Path('fds_bot.db')

if not DB.exists():
    print(f"Database {DB} not found in current directory: {Path.cwd()}")
    sys.exit(1)

action = 'show' if len(sys.argv) < 2 else sys.argv[1].lower()
if action not in ('show', 'clear', 'drop'):
    print('Usage: clear_channels.py [show|clear|drop]')
    sys.exit(2)

con = sqlite3.connect(str(DB))
cur = con.cursor()

# show schema
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print('Tables:', tables)

if 'channels' not in tables:
    print('No channels table found. Exiting.')
    con.close()
    sys.exit(0)

if action == 'show':
    print('\nchannels schema:')
    colinfo = list(cur.execute("PRAGMA table_info('channels')"))
    for row in colinfo:
        print(row)

    cols = [r[1] for r in colinfo]
    print('\nFirst 50 rows (showing existing columns):')
    if not cols:
        print('  no columns found')
    else:
        sel = ','.join(f'"{c}"' for c in cols)
        rows = list(cur.execute(f'SELECT {sel} FROM channels LIMIT 50'))
        # print header
        print('  columns:', cols)
        for r in rows:
            # pretty print as (col:value,...)
            print({cols[i]: r[i] for i in range(len(cols))})
    con.close()
    sys.exit(0)

if action == 'clear':
    cur.execute("UPDATE channels SET name = '', slug = '' WHERE 1")
    con.commit()
    print('Channels: name and slug cleared for all rows')
    con.close()
    sys.exit(0)

if action == 'drop':
    cur.execute('DELETE FROM channels')
    con.commit()
    print('Channels table: all rows deleted')
    con.close()
    sys.exit(0)
