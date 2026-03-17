import sqlite3

DB_PATH = r"C:\Users\user\OneDrive - khwarizme.com\Desktop\lego-tracker\app\lego_tracker.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Remove all new_arrival/new_in_store — these will now come from store collections
cur.execute("DELETE FROM alerts WHERE alert_type IN ('new_arrival', 'new_in_store')")
print(f"Removed {cur.rowcount} new_arrival/new_in_store alerts")

# Remove fake price increases (sale ending = new_price equals compare_at)
cur.execute("""
    DELETE FROM alerts 
    WHERE alert_type='price_increase'
    AND EXISTS (
        SELECT 1 FROM snapshots s 
        WHERE s.item_number = alerts.item_number 
        AND s.store = alerts.store
        AND s.compare_at IS NOT NULL
        AND ABS(alerts.new_price - s.compare_at) < 0.02
        LIMIT 1
    )
""")
print(f"Removed {cur.rowcount} sale-ending price_increase alerts")

cur.execute("SELECT alert_type, COUNT(*) as c FROM alerts GROUP BY alert_type")
print("\nRemaining alerts:")
for r in cur.fetchall():
    print(f"  {r['alert_type']}: {r['c']}")

cur.execute("SELECT COUNT(*) as c FROM alerts")
print(f"Total: {cur.fetchone()['c']}")

conn.commit()
conn.close()
print("\nDone. New arrivals will now come from store collections on next Refresh.")
