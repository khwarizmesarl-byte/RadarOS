"""
BrickRadar DB Cleanup Script
- Removes duplicate snapshot rows (keeps latest per item+store+captured_at)
- Removes old snapshots keeping only the last 10 refreshes per item+store
Run once after updating to the new main.py
"""

import sqlite3
import os

DB_PATH = r"C:\Users\user\OneDrive - khwarizme.com\Desktop\lego-tracker\app\lego_tracker.db"

if not os.path.exists(DB_PATH):
    print(f"DB not found at: {DB_PATH}")
    print("Edit DB_PATH in this script to match your install location.")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("=== BrickRadar DB Cleanup ===\n")

# Count before
cur.execute("SELECT COUNT(*) FROM snapshots")
before = cur.fetchone()[0]
print(f"Rows before cleanup: {before:,}")

# Step 1: Remove exact duplicates (same item_number, store, captured_at, price)
print("\n[1/3] Removing exact duplicate rows...")
cur.execute("""
    DELETE FROM snapshots
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM snapshots
        GROUP BY item_number, store, captured_at
    )
""")
removed_dupes = cur.rowcount
print(f"  Removed: {removed_dupes:,} duplicate rows")

# Step 2: Keep only the last 10 captured_at timestamps per store
print("\n[2/3] Keeping only last 10 refreshes per store...")
cur.execute("SELECT DISTINCT store FROM snapshots")
stores = [r[0] for r in cur.fetchall()]
total_old = 0
for store in stores:
    cur.execute("""
        SELECT DISTINCT captured_at FROM snapshots
        WHERE store=? ORDER BY captured_at DESC
    """, (store,))
    timestamps = [r[0] for r in cur.fetchall()]
    if len(timestamps) > 10:
        to_delete = timestamps[10:]
        placeholders = ",".join("?" * len(to_delete))
        cur.execute(f"DELETE FROM snapshots WHERE store=? AND captured_at IN ({placeholders})",
                    [store] + to_delete)
        total_old += cur.rowcount
        print(f"  {store}: removed {cur.rowcount:,} old rows ({len(timestamps)} → 10 refreshes)")
print(f"  Total old rows removed: {total_old:,}")

# Step 3: Add unique index if not exists
print("\n[3/3] Adding unique index to prevent future duplicates...")
try:
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_unique
        ON snapshots(item_number, store, captured_at)
    """)
    print("  Unique index created OK")
except Exception as e:
    print(f"  Index already exists or error: {e}")

# Count after
cur.execute("SELECT COUNT(*) FROM snapshots")
after = cur.fetchone()[0]

conn.commit()
conn.close()

print("\n=== Done ===")
print(f"Rows before: {before:,}")
print(f"Rows after:  {after:,}")
print(f"Removed:     {before - after:,} rows")
print("\nYou can now start BrickRadar normally.")
