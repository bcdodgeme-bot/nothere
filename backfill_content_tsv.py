#!/usr/bin/env python3
"""
Backfill pages.content_tsv in batches (Step 2 of the content_tsv migration).

Safe to interrupt (Ctrl-C) and re-run: it walks the table by id and only
touches rows where content_tsv IS NULL, so a restart resumes where it
stopped. Each batch is its own transaction, so no long-lived locks.

Run the trigger step (Step 3) BEFORE this script, so that rows written by the
crawler during the backfill are populated by the trigger rather than left NULL
behind the cursor.

Usage:
    python3 backfill_content_tsv.py                  # run the backfill
    python3 backfill_content_tsv.py --status         # progress only, no writes
    python3 backfill_content_tsv.py --batch-size 5000
    python3 backfill_content_tsv.py --start-id 250000   # force a resume point
    python3 backfill_content_tsv.py --sleep 0.25     # throttle (seconds/batch)
"""

import argparse
import os
import sys
import time

import psycopg2

# Same connection handling as app.py
DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# Bound pathological content (base64/minified blobs) before tokenizing
CONTENT_LIMIT = 1000000


def connect():
    if not DATABASE_URL:
        sys.exit('DATABASE_URL is not set')
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cursor = conn.cursor()
    # Generous per-statement ceiling: one batch should take well under this,
    # but the default 15s the web app uses is too tight for a 10k-row update.
    cursor.execute("SET statement_timeout = '10min'")
    cursor.close()
    conn.commit()
    return conn


def print_status(conn):
    """Report how much is left, without writing anything."""
    cursor = conn.cursor()
    cursor.execute("SELECT min(id), max(id) FROM pages")
    lo, hi = cursor.fetchone()
    # Exact count of remaining rows; on 4.8M rows this is a seq scan of a few
    # seconds. Use --status sparingly rather than in a tight loop.
    cursor.execute("SELECT count(*) FROM pages WHERE content_tsv IS NULL")
    remaining = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM pages")
    total = cursor.fetchone()[0]
    cursor.close()
    conn.commit()

    done = total - remaining
    pct = (done / total * 100) if total else 100.0
    print(f'id range      : {lo} .. {hi}')
    print(f'rows total    : {total:,}')
    print(f'rows populated: {done:,} ({pct:.2f}%)')
    print(f'rows remaining: {remaining:,}')
    return lo, hi, remaining


def backfill(conn, batch_size, start_id, sleep_secs):
    cursor = conn.cursor()
    cursor.execute("SELECT coalesce(max(id), 0) FROM pages")
    max_id = cursor.fetchone()[0]
    cursor.close()
    conn.commit()

    if start_id is None:
        # Resume point: the lowest id still needing work. Uses the id PK index.
        cursor = conn.cursor()
        cursor.execute("SELECT min(id) FROM pages WHERE content_tsv IS NULL")
        start_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit()
        if start_id is None:
            print('Nothing to do: content_tsv is fully populated.')
            return

    print(f'Backfilling ids {start_id}..{max_id} in batches of {batch_size:,}')
    print('Ctrl-C is safe: the current batch rolls back, committed batches stay.\n')

    current = start_id
    updated_total = 0
    started = time.time()

    while current <= max_id:
        upper = current + batch_size
        batch_start = time.time()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE pages
                SET content_tsv = to_tsvector('english',
                                              left(coalesce(content, ''), %s))
                WHERE id >= %s AND id < %s
                  AND content_tsv IS NULL
                """,
                (CONTENT_LIMIT, current, upper)
            )
            n = cursor.rowcount
            conn.commit()
        except KeyboardInterrupt:
            conn.rollback()
            print(f'\nInterrupted. Resume with: --start-id {current}')
            raise
        except Exception as e:
            conn.rollback()
            print(f'\nBatch {current}..{upper} failed: {e}')
            print(f'Resume with: --start-id {current}')
            raise
        finally:
            cursor.close()

        updated_total += n
        elapsed = time.time() - batch_start
        pct = min(100.0, (current - start_id) / max(1, max_id - start_id) * 100)
        print(f'  ids {current:>10,}..{upper:>10,}  updated {n:>6,}  '
              f'{elapsed:5.1f}s  [{pct:5.1f}% of id range]', flush=True)

        current = upper
        if sleep_secs:
            time.sleep(sleep_secs)

    total_elapsed = time.time() - started
    print(f'\nDone. Updated {updated_total:,} rows in {total_elapsed/60:.1f} min.')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--batch-size', type=int, default=10000)
    ap.add_argument('--start-id', type=int, default=None,
                    help='force a resume point (default: lowest NULL id)')
    ap.add_argument('--sleep', type=float, default=0.0,
                    help='seconds to pause between batches, to throttle I/O')
    ap.add_argument('--status', action='store_true',
                    help='print progress and exit without writing')
    args = ap.parse_args()

    conn = connect()
    try:
        if args.status:
            print_status(conn)
            return
        backfill(conn, args.batch_size, args.start_id, args.sleep)
    except KeyboardInterrupt:
        sys.exit(130)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
