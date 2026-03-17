import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ── Connection ─────────────────────────────────────────────────────────────────

def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Schema ─────────────────────────────────────────────────────────────────────

def db_init(db_path: str) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS snapshots (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        captured_at  TEXT NOT NULL,
        store        TEXT NOT NULL,
        item_number  TEXT NOT NULL,
        title        TEXT,
        theme        TEXT,
        category     TEXT,
        brand        TEXT,
        price        REAL,
        availability TEXT,
        link         TEXT,
        image_url    TEXT,
        images_json  TEXT,
        compare_at   REAL,
        stock_qty    INTEGER,
        UNIQUE(item_number, store, captured_at)
    )
    """)

    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_unique ON snapshots(item_number, store, captured_at)")
    except Exception:
        pass

    # Migrations for existing DBs
    for col, typedef in [
        ("brand",       "TEXT"),
        ("images_json", "TEXT"),
        ("stock_qty",   "INTEGER"),
    ]:
        try:
            cur.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
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

    try:
        cur.execute("ALTER TABLE alerts ADD COLUMN alert_type TEXT NOT NULL DEFAULT 'price_change'")
    except Exception:
        pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stores (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        name                    TEXT UNIQUE NOT NULL,
        base_url                TEXT NOT NULL,
        platform                TEXT NOT NULL DEFAULT 'shopify',
        vat_multiplier          REAL NOT NULL DEFAULT 1.0,
        new_arrivals_collection TEXT,
        collection_slug         TEXT,
        enabled                 INTEGER NOT NULL DEFAULT 1,
        product_count           INTEGER DEFAULT 0,
        last_scraped            TEXT,
        added_at                TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    for col, typedef in [
        ("new_arrivals_collection", "TEXT"),
        ("collection_slug",         "TEXT"),
        ("lego_only",               "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            cur.execute(f"ALTER TABLE stores ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS radarlist (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        item_number TEXT NOT NULL UNIQUE,
        title       TEXT,
        brand       TEXT,
        theme       TEXT,
        added_at    TEXT NOT NULL DEFAULT (datetime('now')),
        added_price REAL,
        added_store TEXT
    )
    """)

    conn.commit()
    conn.close()


# ── Meta ───────────────────────────────────────────────────────────────────────

def meta_set(db_path: str, key: str, value: str) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value)
    )
    conn.commit()
    conn.close()


def meta_get(db_path: str, key: str) -> Optional[str]:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["v"] if row else None


# ── Alerts ─────────────────────────────────────────────────────────────────────

def alerts_unread_count(db_path: str) -> int:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM alerts WHERE unread=1")
    row = cur.fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def alerts_mark_read(db_path: str) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE alerts SET unread=0 WHERE unread=1")
    conn.commit()
    conn.close()


def compute_alerts(db_path: str, captured_at: str, store: str, catalog: Dict[str, Dict[str, Any]]) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()

    for item, rec in catalog.items():
        offer = (rec.get("stores") or {}).get(store)
        if not offer or offer.price is None:
            continue

        title = rec.get("title") or ""

        cur.execute(
            "SELECT COUNT(*) as cnt FROM snapshots WHERE store=? AND item_number=?",
            (store, item)
        )
        cnt = cur.fetchone()["cnt"]

        is_new_from_store = rec.get("is_new", False)
        if is_new_from_store:
            cur.execute(
                "SELECT 1 FROM alerts WHERE item_number=? AND store=? AND alert_type='new_arrival' LIMIT 1",
                (item, store)
            )
            if not cur.fetchone():
                cur.execute("""
                INSERT INTO alerts(created_at, item_number, store, title, old_price, new_price, alert_type, unread)
                VALUES(?,?,?,?,?,?,?,1)
                """, (captured_at, item, store, title, None, float(offer.price), "new_arrival"))

        if cnt <= 1:
            continue

        cur.execute("""
        SELECT price FROM snapshots
        WHERE store=? AND item_number=?
        ORDER BY id DESC LIMIT 1 OFFSET 1
        """, (store, item))
        prev = cur.fetchone()

        if not prev or prev["price"] is None:
            continue

        old_price = float(prev["price"])
        new_price = float(offer.price)
        diff = new_price - old_price

        if abs(diff) >= 0.01:
            compare_at = rec.get("compare_at")
            if diff > 0 and compare_at and abs(new_price - float(compare_at)) < 0.02:
                continue
            if diff > 0 and diff / old_price > 0.5:
                continue
            alert_type = "price_drop" if diff < 0 else "price_increase"
            cur.execute("""
            INSERT INTO alerts(created_at, item_number, store, title, old_price, new_price, alert_type, unread)
            VALUES(?,?,?,?,?,?,?,1)
            """, (captured_at, item, store, title, old_price, new_price, alert_type))

    conn.commit()
    conn.close()


# ── Snapshots ──────────────────────────────────────────────────────────────────

def persist_snapshot(db_path: str, captured_at: str, store: str, catalog: Dict[str, Dict[str, Any]]) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()

    for item, rec in catalog.items():
        offer = (rec.get("stores") or {}).get(store)
        if not offer:
            continue
        cur.execute("""
        INSERT OR IGNORE INTO snapshots(
            captured_at, store, item_number, title, theme, category, brand,
            price, availability, link, image_url, images_json, compare_at, stock_qty
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            captured_at,
            store,
            item,
            rec.get("title") or "",
            rec.get("theme") or "",
            rec.get("category") or "",
            rec.get("brand") or "",
            offer.price,
            offer.availability,
            offer.link,
            rec.get("image_url") or "",
            json.dumps(rec.get("image_list") or []),
            rec.get("compare_at"),
            offer.stock_qty,
        ))

    conn.commit()
    conn.close()


def latest_snapshot_filter() -> str:
    """SQL snippet: restrict to the most recent captured_at per store."""
    return """
        AND captured_at IN (
            SELECT MAX(captured_at) FROM snapshots GROUP BY store
        )
    """


# ── Stores (DB-managed) ────────────────────────────────────────────────────────

def get_db_stores(db_path: str) -> List[sqlite3.Row]:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT name, base_url, platform, vat_multiplier,
               new_arrivals_collection, collection_slug, lego_only
        FROM stores WHERE enabled=1
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


# ── RadarList ──────────────────────────────────────────────────────────────────

def radarlist_get_ids(db_path: str) -> List[str]:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT item_number FROM radarlist")
    ids = [r[0] for r in cur.fetchall()]
    conn.close()
    return ids


def radarlist_add(db_path: str, item_number: str, title: str, brand: str,
                  theme: str, price: Optional[float], store: Optional[str]) -> bool:
    conn = db_connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
        INSERT INTO radarlist(item_number, title, brand, theme, added_price, added_store)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(item_number) DO NOTHING
        """, (item_number, title, brand, theme, price, store))
        conn.commit()
        added = cur.rowcount > 0
    except Exception:
        added = False
    finally:
        conn.close()
    return added


def radarlist_remove(db_path: str, item_number: str) -> None:
    conn = db_connect(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM radarlist WHERE item_number=?", (item_number,))
    conn.commit()
    conn.close()
