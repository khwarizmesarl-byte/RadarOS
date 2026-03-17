# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 12:19:31 2026

@author: user
"""

import sqlite3

DB_PATH = r"C:\Users\user\OneDrive - khwarizme.com\Desktop\lego-tracker\app\lego_tracker.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Check price history for item #10006 at Brickmania
print("=== Price history: Brickmania #10006 ===")
cur.execute("""
    SELECT id, captured_at, price FROM snapshots
    WHERE store='Brickmania' AND item_number='10006'
    ORDER BY id
""")
for r in cur.fetchall():
    print(f"  id={r['id']} at={r['captured_at']} price={r['price']}")

# Check price history for item #1000
print("\n=== Price history: BRICKSHOP #1000 ===")
cur.execute("""
    SELECT id, captured_at, price FROM snapshots
    WHERE store='BRICKSHOP' AND item_number='1000'
    ORDER BY id
""")
for r in cur.fetchall():
    print(f"  id={r['id']} at={r['captured_at']} price={r['price']}")

# Manually simulate compute_alerts for one item
print("\n=== Simulating compute_alerts for Brickmania #10006 ===")
cur.execute("""
    SELECT COUNT(*) as cnt FROM snapshots
    WHERE store='Brickmania' AND item_number='10006'
""")
cnt = cur.fetchone()['cnt']
print(f"  Count: {cnt}")

if cnt > 1:
    cur.execute("""
        SELECT price FROM snapshots
        WHERE store='Brickmania' AND item_number='10006'
        ORDER BY id DESC
        LIMIT 1 OFFSET 1
    """)
    prev = cur.fetchone()
    print(f"  Previous price (OFFSET 1): {prev['price'] if prev else 'None'}")

    cur.execute("""
        SELECT price FROM snapshots
        WHERE store='Brickmania' AND item_number='10006'
        ORDER BY id DESC
        LIMIT 1
    """)
    latest = cur.fetchone()
    print(f"  Latest price: {latest['price'] if latest else 'None'}")

conn.close()