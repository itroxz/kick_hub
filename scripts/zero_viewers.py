#!/usr/bin/env python3
"""
Zero viewer-like columns in local SQLite DB files in the repo root.

Usage (PowerShell):
    cd C:/Users/Sam/Documents/programa/fds_bot
    py ./scripts/zero_viewers.py

This script will look for common monitor DB filenames in the current working
directory (for example `kick_monitor.sqlite3` and `fds_bot.db`) and set any
columns named `viewers`, `peak_viewers`, `max_viewers` (if present) to 0.

It prints a brief before/after count for columns it updates. It does not create
backups (as requested). Use with caution.
"""
import sqlite3
from pathlib import Path
import sys

DB_CANDIDATES = [
    'kick_monitor.sqlite3',
    'kick_monitor.sqlite',
    'kick_monitor.db',
    'fds_bot.db',
]

COLS_TO_ZERO = ['viewers', 'peak_viewers', 'max_viewers']
# columns that contain usernames/channel identifiers; these will be set to NULL
USER_COLS = ['channel', 'username', 'user', 'streamer', 'slug', 'display_name']


def process_db(path: Path):
    if not path.exists():
        print(f"[skip] {path} not found")
        return

    print(f"Processing {path}")
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()

    # get tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        print("  no tables found")

    for t in tables:
        # get columns for the table
        cur.execute(f"PRAGMA table_info('{t}')")
        cols = [r[1] for r in cur.fetchall()]

        for col in COLS_TO_ZERO:
            if col in cols:
                try:
                    # count how many rows would be affected
                    cur.execute(f"SELECT COUNT(*) FROM \"{t}\" WHERE \"{col}\">0")
                    before = cur.fetchone()[0]
                    if before == 0:
                        print(f"  table '{t}': column '{col}' already zero in all rows")
                        continue

                    cur.execute(f"UPDATE \"{t}\" SET \"{col}\" = 0 WHERE \"{col}\">0")
                    conn.commit()
                    cur.execute(f"SELECT COUNT(*) FROM \"{t}\" WHERE \"{col}\">0")
                    after = cur.fetchone()[0]
                    print(f"  table '{t}': column '{col}' before={before} after={after}")
                except Exception as e:
                    print(f"  failed to update {t}.{col}: {e}")

        # nullify username-like columns
        for ucol in USER_COLS:
            if ucol in cols:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM \"{t}\" WHERE \"{ucol}\" IS NOT NULL AND TRIM(\"{ucol}\")<>''")
                    beforeu = cur.fetchone()[0]
                    if beforeu == 0:
                        print(f"  table '{t}': column '{ucol}' already empty/null in all rows")
                        continue

                    # try to set to NULL first
                    try:
                        cur.execute(f"UPDATE \"{t}\" SET \"{ucol}\" = NULL WHERE \"{ucol}\" IS NOT NULL AND TRIM(\"{ucol}\")<>''")
                        conn.commit()
                    except Exception as e:
                        # if NOT NULL constraint prevents NULL, fall back to empty string
                        msg = str(e)
                        if 'NOT NULL constraint' in msg or 'NOT NULL' in msg:
                            try:
                                cur.execute(f"UPDATE \"{t}\" SET \"{ucol}\" = '' WHERE \"{ucol}\" IS NOT NULL AND TRIM(\"{ucol}\")<>''")
                                conn.commit()
                                print(f"  table '{t}': column '{ucol}' could not be NULLed due to NOT NULL constraint; set to empty string instead")
                            except Exception as e2:
                                print(f"  failed to clear {t}.{ucol} (empty-string fallback): {e2}")
                        else:
                            print(f"  failed to nullify {t}.{ucol}: {e}")

                    cur.execute(f"SELECT COUNT(*) FROM \"{t}\" WHERE \"{ucol}\" IS NOT NULL AND TRIM(\"{ucol}\")<>''")
                    afteru = cur.fetchone()[0]
                    print(f"  table '{t}': column '{ucol}' before={beforeu} after={afteru}")
                except Exception as e:
                    print(f"  failed to nullify {t}.{ucol}: {e}")

    conn.close()


def main():
    cwd = Path.cwd()
    print(f"Running in: {cwd}")

    any_found = False
    for name in DB_CANDIDATES:
        p = cwd / name
        if p.exists():
            any_found = True
        process_db(p)

    if not any_found:
        print("No candidate DB files found in the current directory. Exiting.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        sys.exit(1)
