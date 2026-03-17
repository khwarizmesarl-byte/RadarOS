import sqlite3

DB_PATH = r"C:\Users\user\OneDrive - khwarizme.com\Desktop\lego-tracker\app\lego_tracker.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("PRAGMA table_info(alerts)")
print("Alerts columns:", [r["name"] for r in cur.fetchall()])

cur.execute("SELECT COUNT(*) as c FROM alerts")
print("Total alerts:", cur.fetchone()["c"])

cur.execute("SELECT COUNT(*) as c FROM snapshots")
print("Total snapshots:", cur.fetchone()["c"])

cur.execute("""
    SELECT item_number, store, COUNT(*) as cnt 
    FROM snapshots 
    GROUP BY item_number, store 
    HAVING cnt > 1 
    LIMIT 5
""")
rows = cur.fetchall()
print("Items with >1 snapshot:", len(rows))
for r in rows:
    print(f"  {r['store']} #{r['item_number']} — {r['cnt']} snapshots")

cur.execute("SELECT DISTINCT store FROM snapshots")
print("Stores in DB:", [r["store"] for r in cur.fetchall()])

conn.close()
print("Done.")