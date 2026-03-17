import sys
sys.path.insert(0, '.')
from core.db import db_connect

DB = 'Brickradar/app/data/lego_tracker.sqlite3'
conn = db_connect(DB)

migrations = [
    "ALTER TABLE alerts ADD COLUMN old_price REAL",
    "ALTER TABLE alerts ADD COLUMN new_price REAL",
    "ALTER TABLE alerts ADD COLUMN alert_type TEXT NOT NULL DEFAULT 'price_change'",
    "ALTER TABLE alerts ADD COLUMN unread INTEGER NOT NULL DEFAULT 1",
]

for sql in migrations:
    try:
        conn.execute(sql)
        print(f"OK: {sql}")
    except Exception as e:
        print(f"Skip: {e}")

conn.commit()
conn.close()
print("Done")
