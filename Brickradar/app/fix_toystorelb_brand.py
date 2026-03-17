"""
Fix Thetoystorelb brand values in snapshots.
Old snapshots have wrong brand (e.g. 'WEDNESDAY', 'FERRARI') extracted from titles.
Since vendor is 'Lego', all items should be brand='LEGO'.
This script deletes all non-LEGO snapshots for Thetoystorelb so the next scrape
will re-insert them correctly.
"""
import sqlite3

DB = "lego_tracker.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

# Show current brand distribution
print("=== Current brand distribution for Thetoystorelb ===")
rows = cur.execute("""
    SELECT brand, COUNT(*) as cnt
    FROM snapshots
    WHERE store = 'Thetoystorelb'
    GROUP BY brand
    ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  brand={r[0]!r:20s} count={r[1]}")

# Count bad ones
bad = cur.execute("""
    SELECT COUNT(*) FROM snapshots
    WHERE store = 'Thetoystorelb' AND UPPER(brand) != 'LEGO'
""").fetchone()[0]
print(f"\nBad (non-LEGO) snapshots: {bad}")

# Option 1: UPDATE brand to 'LEGO' for all Thetoystorelb snapshots
# (safe since vendor confirms they're all LEGO products)
cur.execute("""
    UPDATE snapshots
    SET brand = 'LEGO'
    WHERE store = 'Thetoystorelb' AND UPPER(brand) != 'LEGO'
""")
updated = cur.rowcount
print(f"Updated {updated} snapshots to brand='LEGO'")

conn.commit()
conn.close()
print("Done.")
