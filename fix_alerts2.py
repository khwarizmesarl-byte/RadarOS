import sys
sys.path.insert(0, '.')
from core.db import db_connect

DB = 'Brickradar/app/data/lego_tracker.sqlite3'
conn = db_connect(DB)

# Check what columns alerts table actually has
cur = conn.cursor()
cur.execute("PRAGMA table_info(alerts)")
cols = cur.fetchall()
print("Current alerts columns:")
for c in cols:
    print(f"  {c['name']} {c['type']} notnull={c['notnull']} dflt={c['dflt_value']}")

# Fix: set a default value for message column if it exists
try:
    # SQLite can't alter constraints, so we recreate the table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at  TEXT NOT NULL,
            item_number TEXT NOT NULL,
            store       TEXT NOT NULL,
            title       TEXT,
            old_price   REAL,
            new_price   REAL,
            alert_type  TEXT NOT NULL DEFAULT 'price_change',
            unread      INTEGER NOT NULL DEFAULT 1
        )
    """)
    # Copy existing data
    conn.execute("""
        INSERT INTO alerts_new(id, created_at, item_number, store, title, old_price, new_price, alert_type, unread)
        SELECT id, created_at, item_number, store,
               COALESCE(title, ''),
               old_price, new_price,
               COALESCE(alert_type, 'price_change'),
               COALESCE(unread, 1)
        FROM alerts
    """)
    conn.execute("DROP TABLE alerts")
    conn.execute("ALTER TABLE alerts_new RENAME TO alerts")
    conn.commit()
    print("\nAlerts table rebuilt successfully.")
except Exception as e:
    print(f"Error: {e}")

conn.close()
