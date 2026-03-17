"""
Migration: add source_type and country_code to snapshots and stores tables.
Tags all existing Lebanese stores as source_type='local', country_code='LB'.
"""
import sys
sys.path.insert(0, '.')
from core.db import db_connect

DB = 'Brickradar/app/data/lego_tracker.sqlite3'
conn = db_connect(DB)
cur  = conn.cursor()

# ── snapshots table ────────────────────────────────────────────────────────────
for sql in [
    "ALTER TABLE snapshots ADD COLUMN source_type TEXT NOT NULL DEFAULT 'local'",
    "ALTER TABLE snapshots ADD COLUMN country_code TEXT NOT NULL DEFAULT 'LB'",
]:
    try:
        cur.execute(sql)
        print(f"OK: {sql}")
    except Exception as e:
        print(f"Skip: {e}")

# ── stores table ───────────────────────────────────────────────────────────────
for sql in [
    "ALTER TABLE stores ADD COLUMN source_type TEXT NOT NULL DEFAULT 'local'",
    "ALTER TABLE stores ADD COLUMN country_code TEXT NOT NULL DEFAULT 'LB'",
]:
    try:
        cur.execute(sql)
        print(f"OK: {sql}")
    except Exception as e:
        print(f"Skip: {e}")

conn.commit()

# ── Tag existing data ──────────────────────────────────────────────────────────
LEBANESE_STORES = [
    "Brickmania", "BRICKSHOP", "Bricking", "PlayOne",
    "KLAPTAP", "Ayoub Computers", "Joueclubliban",
    "Thetoystorelb", "Brix & Figures",
]

cur.execute(
    f"UPDATE snapshots SET source_type='local', country_code='LB' WHERE store IN ({','.join('?'*len(LEBANESE_STORES))})",
    LEBANESE_STORES
)
print(f"Tagged {cur.rowcount} snapshot rows as local/LB")

cur.execute(
    f"UPDATE stores SET source_type='local', country_code='LB' WHERE name IN ({','.join('?'*len(LEBANESE_STORES))})",
    LEBANESE_STORES
)
print(f"Tagged {cur.rowcount} store rows as local/LB")

conn.commit()

# ── Verify ─────────────────────────────────────────────────────────────────────
cur.execute("SELECT source_type, country_code, COUNT(*) as cnt FROM snapshots GROUP BY source_type, country_code")
print("\nSnapshot breakdown:")
for r in cur.fetchall():
    print(f"  {r[0]} / {r[1]}: {r[2]} rows")

cur.execute("SELECT source_type, country_code, COUNT(*) as cnt FROM stores GROUP BY source_type, country_code")
print("\nStore breakdown:")
for r in cur.fetchall():
    print(f"  {r[0]} / {r[1]}: {r[2]} rows")

conn.close()
print("\nDone.")
