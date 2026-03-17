from __future__ import annotations

import csv
import io
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SHOPIFY_LIMIT = 250
MAX_PAGES_DEFAULT = 120  # bump up for bigger catalogs

STORES: Dict[str, Dict[str, Any]] = {
    "Brickmania": {
        "base_url": "https://thebrickmania.com",
        "type": "shopify",
        "vat_multiplier": 1.0,
    },
    "Bricking": {
        "base_url": "https://bricking.com",
        "type": "shopify",
        "vat_multiplier": 1.11,
    },
    # Brickshop not included (not clean Shopify JSON)
}

ITEM_RE = re.compile(r"\b([0-9]{4,7})\b")

APP_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = APP_DIR / "templates"

app = FastAPI(title="LEGO Price Tracker")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# -------------------------------------------------------------------
# UTIL
# -------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_db_path() -> Path:
    env = os.getenv("LEGO_DB_PATH")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (APP_DIR / p).resolve()
        return p

    candidates = [
        (APP_DIR.parent / "lego_tracker.db").resolve(),
        (APP_DIR / "lego_tracker.db").resolve(),
    ]
    for p in candidates:
        if p.exists():
            return p

    return (APP_DIR.parent / "lego_tracker.db").resolve()


DB_PATH = resolve_db_path()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(r)


def _parse_int(v: Optional[str], default: int) -> int:
    try:
        if v is None:
            return default
        return max(1, int(v))
    except Exception:
        return default


def _canon_store(s: str) -> str:
    return (s or "").strip()


def normalize_img(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    return url


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def extract_item_number(title: str, sku: str) -> Optional[str]:
    sku = (sku or "").strip()
    m = ITEM_RE.search(sku)
    if m:
        return m.group(1)

    t = (title or "").strip()
    m = ITEM_RE.search(t)
    return m.group(1) if m else None


def normalize_brand(vendor: str) -> str:
    v = (vendor or "").strip().lower()
    if "lego" in v:
        return "LEGO"
    if "cada" in v:
        return "CADA"
    if "lumibricks" in v or "funwhole" in v:
        return "LUMIBRICKS"
    if v == "mega" or "mega" in v:
        return "MEGA"
    token = re.split(r"[/()\-]", v)[0].strip()
    return (token or "UNKNOWN").upper()


def display_item_number(item_number: str) -> str:
    return (item_number or "").strip()


# -------------------------------------------------------------------
# DB SCHEMA
# -------------------------------------------------------------------
def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_number TEXT NOT NULL,
                title TEXT,
                theme TEXT,
                category TEXT,
                image_url TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offers(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                store TEXT NOT NULL,
                price REAL,
                regular_price REAL,
                discount_pct INTEGER,
                availability TEXT,
                link TEXT,
                last_seen_at TEXT,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                store TEXT NOT NULL,
                price REAL,
                regular_price REAL,
                discount_pct INTEGER,
                availability TEXT,
                link TEXT,
                captured_at TEXT,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                store TEXT,
                target_price REAL,
                created_at TEXT,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )

        ensure_schema(conn)
        conn.commit()


def ensure_schema(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "products"):
        return

    cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
    if "brand" not in cols:
        conn.execute("ALTER TABLE products ADD COLUMN brand TEXT NOT NULL DEFAULT 'LEGO'")
        conn.commit()

    # Offers should be unique per (product_id, store)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_offers_product_store ON offers(product_id, store)")
    conn.commit()


# -------------------------------------------------------------------
# SHOPIFY FETCH
# -------------------------------------------------------------------
def shopify_products_endpoints(base_url: str, page: int) -> List[str]:
    return [
        f"{base_url}/products.json?limit={SHOPIFY_LIMIT}&page={page}",
        f"{base_url}/collections/all/products.json?limit={SHOPIFY_LIMIT}&page={page}",
    ]


async def fetch_shopify_all(base_url: str, max_pages: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            data = None
            for url in shopify_products_endpoints(base_url, page):
                try:
                    r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    if r.status_code != 200:
                        continue
                    j = r.json()
                    if isinstance(j, dict) and "products" in j:
                        data = j
                        break
                except Exception:
                    continue

            if not data:
                break

            products = data.get("products") or []
            if not products:
                break

            out.extend(products)

            if len(products) < SHOPIFY_LIMIT:
                break

    return out


def calc_discount_pct(price: Optional[float], regular: Optional[float]) -> Optional[int]:
    if price is None or regular is None or regular <= 0:
        return None
    if price >= regular:
        return 0
    return int(round((1.0 - (price / regular)) * 100))


def upsert_product(
    conn: sqlite3.Connection,
    brand: str,
    item_number: str,
    title: str,
    theme: str,
    category: str,
    image_url: Optional[str],
) -> int:
    row = conn.execute("SELECT id FROM products WHERE item_number=?", (item_number,)).fetchone()
    if row:
        pid = int(row["id"])
        conn.execute(
            "UPDATE products SET brand=?, title=?, theme=?, category=?, image_url=? WHERE id=?",
            (brand, title, theme, category, image_url, pid),
        )
        return pid

    try:
        cur = conn.execute(
            "INSERT INTO products(brand, item_number, title, theme, category, image_url) VALUES(?,?,?,?,?,?)",
            (brand, item_number, title, theme, category, image_url),
        )
        return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        row = conn.execute("SELECT id FROM products WHERE item_number=?", (item_number,)).fetchone()
        if not row:
            raise
        pid = int(row["id"])
        conn.execute(
            "UPDATE products SET brand=?, title=?, theme=?, category=?, image_url=? WHERE id=?",
            (brand, title, theme, category, image_url, pid),
        )
        return pid


def upsert_offer(
    conn: sqlite3.Connection,
    product_id: int,
    store: str,
    price: Optional[float],
    regular_price: Optional[float],
    discount_pct: Optional[int],
    availability: str,
    link: Optional[str],
) -> None:
    ts = now_iso()

    conn.execute(
        """
        INSERT INTO offers(product_id, store, price, regular_price, discount_pct, availability, link, last_seen_at)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(product_id, store) DO UPDATE SET
            price=excluded.price,
            regular_price=excluded.regular_price,
            discount_pct=excluded.discount_pct,
            availability=excluded.availability,
            link=excluded.link,
            last_seen_at=excluded.last_seen_at
        """,
        (product_id, store, price, regular_price, discount_pct, availability, link, ts),
    )

    conn.execute(
        """
        INSERT INTO price_history(product_id, store, price, regular_price, discount_pct, availability, link, captured_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (product_id, store, price, regular_price, discount_pct, availability, link, ts),
    )


def pick_best_variant(variants: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Optional[float], bool]:
    """
    Best = available + lowest valid numeric price.
    If no numeric price exists, fallback to first variant with price=None.
    """
    best: Optional[Dict[str, Any]] = None
    best_price: Optional[float] = None
    best_available = False

    for v in variants:
        pr = _to_float(v.get("price"))
        if pr is None:
            continue
        available = bool(v.get("available", True))

        if best is None:
            best = v
            best_price = pr
            best_available = available
            continue

        if available and not best_available:
            best = v
            best_price = pr
            best_available = available
            continue

        if available == best_available and best_price is not None and pr < best_price:
            best = v
            best_price = pr
            best_available = available

    if best is None:
        v0 = variants[0]
        return v0, None, bool(v0.get("available", True))

    return best, best_price, best_available


async def refresh_all_stores(max_pages: int = MAX_PAGES_DEFAULT) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    with db() as conn:
        ensure_schema(conn)

        for store, cfg in STORES.items():
            if cfg.get("type") != "shopify":
                continue

            base_url = str(cfg["base_url"]).rstrip("/")
            vat = float(cfg.get("vat_multiplier", 1.0))

            products = await fetch_shopify_all(base_url, max_pages=max_pages)
            counts[store] = 0

            for p in products:
                title = (p.get("title") or "").strip()
                vendor = (p.get("vendor") or "").strip()
                brand = normalize_brand(vendor)

                theme = (p.get("product_type") or "").strip()
                category = ""  # optional mapping later

                img = None
                images = p.get("images") or []
                if images:
                    img = normalize_img((images[0] or {}).get("src"))
                else:
                    img = normalize_img((p.get("image") or {}).get("src"))

                handle = (p.get("handle") or "").strip()
                link = f"{base_url}/products/{handle}" if handle else base_url

                variants = p.get("variants") or []
                if not variants:
                    continue

                best, best_price, best_available = pick_best_variant(variants)

                sku = (best.get("sku") or "").strip()
                item = extract_item_number(title, sku)
                if not item:
                    continue

                price = best_price
                regular = _to_float(best.get("compare_at_price"))

                if price is not None:
                    price = round(price * vat, 2)
                if regular is not None:
                    regular = round(regular * vat, 2)

                availability = "In stock" if bool(best_available) else "Out of stock"
                discount_pct = calc_discount_pct(price, regular)

                pid = upsert_product(conn, brand, item, title, theme, category, img)
                # IMPORTANT: even if price=None, we still store offer so UI shows availability + open link
                upsert_offer(conn, pid, store, price, regular, discount_pct, availability, link)

                counts[store] += 1

        conn.commit()

    return counts


# -------------------------------------------------------------------
# QUERY LAYER
# -------------------------------------------------------------------
def get_stores(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT store AS v FROM offers WHERE store IS NOT NULL AND TRIM(store) <> '' ORDER BY v"
    ).fetchall()
    return [_canon_store(r["v"]) for r in rows]


def get_distinct_products_field(conn: sqlite3.Connection, field: str) -> List[str]:
    sql = f"""
        SELECT DISTINCT {field} AS v
        FROM products
        WHERE {field} IS NOT NULL AND TRIM({field}) <> ''
        ORDER BY v
    """
    return [r["v"] for r in conn.execute(sql).fetchall()]


def fetch_products_with_offers(
    conn: sqlite3.Connection,
    stores: List[str],
    category: str,
    theme: str,
    q: str,
) -> List[sqlite3.Row]:
    if not stores:
        return []

    params: Dict[str, Any] = {}
    store_placeholders = []
    for i, s in enumerate(stores):
        params[f"s{i}"] = s
        store_placeholders.append(f":s{i}")
    store_in = ",".join(store_placeholders)

    pw = []
    if category:
        pw.append("p.category = :category")
        params["category"] = category
    if theme:
        pw.append("p.theme = :theme")
        params["theme"] = theme
    if q:
        pw.append("(p.title LIKE :q OR p.item_number LIKE :q OR p.brand LIKE :q)")
        params["q"] = f"%{q}%"

    products_where = (" AND " + " AND ".join(pw)) if pw else ""

    # Key fix:
    # - We only list products that have at least one offer in selected stores (EXISTS)
    # - We join offers filtered to selected stores (no store=None ghost rows)
    sql = f"""
        SELECT
            p.id AS product_id,
            p.brand,
            p.item_number,
            p.title,
            p.theme,
            p.category,
            p.image_url,
            o.store,
            o.price,
            o.regular_price,
            o.discount_pct,
            o.availability,
            o.link,
            o.last_seen_at
        FROM products p
        LEFT JOIN offers o
            ON o.product_id = p.id
           AND o.store IN ({store_in})
        WHERE 1=1
          {products_where}
          AND EXISTS (
              SELECT 1 FROM offers o2
              WHERE o2.product_id = p.id
                AND o2.store IN ({store_in})
          )
    """
    return conn.execute(sql, params).fetchall()


def merge_products(rows: List[sqlite3.Row], selected_stores: List[str]) -> List[Dict[str, Any]]:
    merged: Dict[int, Dict[str, Any]] = {}

    for r in rows:
        pid = int(r["product_id"])
        row = merged.get(pid)
        if row is None:
            row = {
                "product_id": pid,
                "brand": (r["brand"] or "LEGO").strip(),
                "item_number": (r["item_number"] or "").strip(),
                "item_number_display": display_item_number((r["item_number"] or "").strip()),
                "title": r["title"] or "",
                "theme": r["theme"] or "",
                "category": r["category"] or "",
                "image_url": normalize_img(r["image_url"]) if r["image_url"] else "",
                "offers": {},
                "lowest_price": None,
                "lowest_store": "",
            }
            merged[pid] = row

        store = _canon_store(r["store"] or "")
        if store:
            row["offers"][store] = {
                "price": r["price"],
                "regular_price": r["regular_price"],
                "discount_pct": r["discount_pct"],
                "availability": r["availability"] or "",
                "link": r["link"] or "",
                "last_seen_at": r["last_seen_at"] or "",
            }

    out = list(merged.values())

    for row in out:
        lowest_price = None
        lowest_store = ""
        for s in selected_stores:
            off = row["offers"].get(s)
            if not off:
                continue
            p = off.get("price")
            if p is None:
                continue
            try:
                pf = float(p)
            except Exception:
                continue
            if lowest_price is None or pf < lowest_price:
                lowest_price = pf
                lowest_store = s
        row["lowest_price"] = lowest_price
        row["lowest_store"] = lowest_store

    return out


def sort_rows(rows: List[Dict[str, Any]], sort: str, order: str) -> List[Dict[str, Any]]:
    reverse = (order or "asc").lower() == "desc"
    s = (sort or "item_number").strip()

    def k_item(r): return (r.get("item_number") or "")
    def k_brand(r): return ((r.get("brand") or "").lower(), r.get("item_number") or "")
    def k_title(r): return ((r.get("title") or "").lower(), r.get("item_number") or "")
    def k_cat(r): return ((r.get("category") or "").lower(), r.get("item_number") or "")
    def k_theme(r): return ((r.get("theme") or "").lower(), r.get("item_number") or "")

    def k_lowest(r):
        v = r.get("lowest_price")
        return (v is None, v if v is not None else 10**18, r.get("item_number") or "")

    if s.startswith("store:"):
        store = s.split(":", 1)[1]

        def k_store(r):
            off = r.get("offers", {}).get(store)
            p = off.get("price") if off else None
            try:
                p = float(p) if p is not None else None
            except Exception:
                p = None
            return (p is None, p if p is not None else 10**18, r.get("item_number") or "")

        return sorted(rows, key=k_store, reverse=reverse)

    if s == "brand":
        return sorted(rows, key=k_brand, reverse=reverse)
    if s == "title":
        return sorted(rows, key=k_title, reverse=reverse)
    if s == "category":
        return sorted(rows, key=k_cat, reverse=reverse)
    if s == "theme":
        return sorted(rows, key=k_theme, reverse=reverse)
    if s == "lowest":
        return sorted(rows, key=k_lowest, reverse=reverse)

    return sorted(rows, key=k_item, reverse=reverse)


def build_sort_urls(
    request: Request,
    selected_stores: List[str],
    current_sort: str,
    current_order: str,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    from urllib.parse import urlencode

    qp = request.query_params.multi_items()

    def toggle_order(target_sort: str) -> str:
        if (current_sort or "") == target_sort:
            return "desc" if (current_order or "asc") == "asc" else "asc"
        return "asc"

    def make_url(sort: str, order: str) -> str:
        items = [(k, v) for (k, v) in qp if k not in ("sort", "order", "page")]
        items.append(("sort", sort))
        items.append(("order", order))
        items.append(("page", "1"))
        return "/?" + urlencode(items, doseq=True)

    arrows: Dict[str, str] = {}
    urls: Dict[str, str] = {}

    def arrow_for(col: str) -> str:
        if current_sort != col:
            return ""
        return "▲" if (current_order or "asc") == "asc" else "▼"

    for col in ["brand", "item_number", "title", "category", "theme", "lowest"]:
        urls[col] = make_url(col, toggle_order(col))
        arrows[col] = arrow_for(col)

    for s in selected_stores:
        col = f"store:{s}"
        urls[col] = make_url(col, toggle_order(col))
        arrows[col] = arrow_for(col)

    return urls, arrows


# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------
@app.on_event("startup")
async def _startup() -> None:
    init_db()
    await refresh_all_stores()


@app.get("/debug/db", response_class=PlainTextResponse)
def debug_db() -> PlainTextResponse:
    with db() as conn:
        tables = [r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return PlainTextResponse("DB_PATH=" + str(DB_PATH) + "\n" + "tables=" + ", ".join(tables))


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    with db() as conn:
        for t in ["products", "offers", "price_history"]:
            if not table_exists(conn, t):
                return PlainTextResponse(
                    f"DB schema mismatch. Missing table '{t}'. Using DB_PATH={DB_PATH}\n"
                    f"Open /debug/db to verify.\n"
                    f"If wrong DB, set env var LEGO_DB_PATH to your real file.\n",
                    status_code=500,
                )

        all_stores = get_stores(conn)

        selected_stores = [s for s in request.query_params.getlist("stores") if s]
        if not selected_stores:
            selected_stores = all_stores[:]

        category = (request.query_params.get("category") or "").strip()
        theme = (request.query_params.get("theme") or "").strip()
        query = (request.query_params.get("q") or "").strip()

        per_page = _parse_int(request.query_params.get("per_page"), 50)
        page = _parse_int(request.query_params.get("page"), 1)

        sort = (request.query_params.get("sort") or "item_number").strip()
        order = (request.query_params.get("order") or "asc").strip().lower()
        if order not in ("asc", "desc"):
            order = "asc"

        raw = fetch_products_with_offers(
            conn,
            selected_stores,
            "" if category in ("", "All") else category,
            "" if theme in ("", "All") else theme,
            query,
        )

        merged = merge_products(raw, selected_stores)
        rows = sort_rows(merged, sort, order)

        total = len(rows)
        start = (page - 1) * per_page
        end = start + per_page
        page_rows = rows[start:end]

        all_categories = ["All"] + get_distinct_products_field(conn, "category")
        all_themes = ["All"] + get_distinct_products_field(conn, "theme")

        sort_urls, sort_arrows = build_sort_urls(request, selected_stores, sort, order)

        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "rows": page_rows,
                "total": total,
                "page": page,
                "per_page": per_page,
                "category": category or "All",
                "theme": theme or "All",
                "q": query,
                "all_categories": all_categories,
                "all_themes": all_themes,
                "all_stores": all_stores,
                "selected_stores": selected_stores,
                "sort_urls": sort_urls,
                "sort_arrows": sort_arrows,
            },
        )


@app.get("/export.csv")
def export_csv(request: Request):
    with db() as conn:
        all_stores = get_stores(conn)

        selected_stores = [s for s in request.query_params.getlist("stores") if s]
        if not selected_stores:
            selected_stores = all_stores[:]

        category = (request.query_params.get("category") or "").strip()
        theme = (request.query_params.get("theme") or "").strip()
        query = (request.query_params.get("q") or "").strip()

        sort = (request.query_params.get("sort") or "item_number").strip()
        order = (request.query_params.get("order") or "asc").strip().lower()
        if order not in ("asc", "desc"):
            order = "asc"

        raw = fetch_products_with_offers(
            conn,
            selected_stores,
            "" if category in ("", "All") else category,
            "" if theme in ("", "All") else theme,
            query,
        )

        rows = sort_rows(merge_products(raw, selected_stores), sort, order)

        output = io.StringIO()
        cols = (
            ["brand", "item_number", "title", "category", "theme"]
            + [f"{s}_price" for s in selected_stores]
            + ["lowest_price", "lowest_store"]
        )

        w = csv.DictWriter(output, fieldnames=cols)
        w.writeheader()

        for r in rows:
            rec = {
                "brand": r.get("brand", ""),
                "item_number": r.get("item_number", ""),
                "title": r.get("title", ""),
                "category": r.get("category", ""),
                "theme": r.get("theme", ""),
                "lowest_price": r.get("lowest_price", ""),
                "lowest_store": r.get("lowest_store", ""),
            }
            for s in selected_stores:
                off = r.get("offers", {}).get(s)
                rec[f"{s}_price"] = off.get("price") if off else ""
            w.writerow(rec)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        payload = output.getvalue().encode("utf-8")
        headers = {"Content-Disposition": f'attachment; filename="export_{ts}.csv"'}
        return StreamingResponse(io.BytesIO(payload), media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/refresh")
async def refresh() -> RedirectResponse:
    await refresh_all_stores()
    return RedirectResponse(url="/", status_code=303)