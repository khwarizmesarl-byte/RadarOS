import sqlite3
conn = sqlite3.connect("lego_tracker.db")

# Remove collection_slug so it scrapes all products (vendor='Lego' handles filtering)
conn.execute("UPDATE stores SET collection_slug=NULL WHERE name='Thetoystorelb'")
conn.commit()

row = conn.execute("SELECT name, collection_slug, lego_only FROM stores WHERE name='Thetoystorelb'").fetchone()
print(f"Updated: name={row[0]!r} collection_slug={row[1]!r} lego_only={row[2]}")
conn.close()
print("Done. Now refresh the dashboard.")
