import sqlite3

conn = sqlite3.connect("lego_tracker.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

cursor.execute("PRAGMA table_info(state);")
print(cursor.fetchall())

cursor.execute("PRAGMA table_info(snapshots);")
print(cursor.fetchall())

