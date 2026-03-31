"""
modules/brickradar/scrapers_official.py
Scrapers for official brand sources (Tier 1) and international retail (Tier 2).
"""

import re
import json
import httpx
from typing import Any, Dict, Optional
from bs4 import BeautifulSoup

from core.models import StoreOffer
from core.utils import safe_float, extract_item_number, compute_discount_pct
from core.scrapers.shopify import fetch_shopify_store
from modules.brickradar.config import OFFICIAL_STORES, INTERNATIONAL_STORES


# ── Theme normalisation for non-LEGO brands ────────────────────────────────────

def normalize_theme_cada(prod: dict) -> tuple:
    product_type = (prod.get("product_type") or "").strip()
    return product_type or "CaDA", "CaDA"


# ── CaDA Official ──────────────────────────────────────────────────────────────

def fetch_cada() -> Dict[str, Dict[str, Any]]:
    """Scrape CaDA official Shopify store."""
    cfg = OFFICIAL_STORES["CaDA Official"]
    print("[CaDA] scraping official store...")
    result = fetch_shopify_store(
        store_name="CaDA Official",
        base_url=cfg["url"],
        vat_multiplier=cfg.get("vat_multiplier", 1.0),
        collection_slug=cfg.get("collection_slug") or "",
        lego_only=False,
        normalize_theme_fn=normalize_theme_cada,
    )
    print(f"[CaDA] scraped {len(result)} products")
    return result


# ── Generic ueeshop scraper (Reobrix and similar) ─────────────────────────────

def fetch_ueeshop_store(store_name: str, base_url: str,
                        max_pages: int = 200, db_path: str = None) -> Dict[str, Dict[str, Any]]:
    """
    Scrape any ueeshop-based store (e.g. Reobrix).
    Strategy:
      1. Paginate /products/?page=N to collect all product slugs
      2. Fetch each /products/{slug} and extract schema.org JSON
    Uses rotating pagination if db_path provided (stores last_page in meta).
    """
    import httpx as _httpx
    import json as _json
    import re as _re
    from core.utils import safe_float, extract_item_number, compute_discount_pct

    base_url  = base_url.rstrip("/")
    headers   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    meta_key  = f"{store_name.lower().replace(' ','_')}_last_page"
    out: Dict[str, Dict[str, Any]] = {}

    # Load last page from DB meta
    start_page = 1
    if db_path:
        try:
            import sqlite3 as _sq
            conn = _sq.connect(db_path)
            row  = conn.execute("SELECT v FROM meta WHERE k=?", (meta_key,)).fetchone()
            conn.close()
            if row:
                start_page = int(row[0]) + 1
        except: pass

    pages_per_run = 10  # fetch 10 pages (200 products) per run
    end_page      = start_page + pages_per_run - 1
    all_slugs: list = []
    seen_slugs: set = set()
    last_page_scraped = start_page - 1

    print(f"[{store_name}] collecting slugs pages {start_page}–{end_page}...")

    with _httpx.Client(timeout=20, follow_redirects=True, headers=headers) as client:
        for page in range(start_page, end_page + 1):
            try:
                r = client.get(f"{base_url}/products/?page={page}")
                if r.status_code != 200:
                    break
                slugs = set(_re.findall(r"href=[\"'][^\"']*?/products/([\w-]+)[\"']", r.text))
                slugs = {s for s in slugs if s and '.' not in s}
                if not slugs:
                    # Reached end — reset to page 1 for next run
                    print(f"[{store_name}] reached end at page {page} — resetting to page 0")
                    last_page_scraped = 0
                    break
                new_slugs = slugs - seen_slugs
                if not new_slugs:
                    print(f"[{store_name}] duplicate page {page} — resetting")
                    last_page_scraped = 0
                    break
                seen_slugs |= slugs
                all_slugs.extend(new_slugs)
                last_page_scraped = page
            except Exception as e:
                print(f"[{store_name}] page {page} error: {e}")
                break

        print(f"[{store_name}] scraping {len(all_slugs)} product pages...")
        for slug in all_slugs:
            try:
                r = client.get(f"{base_url}/products/{slug}")
                if r.status_code != 200:
                    continue
                # Extract schema.org JSON
                m = _re.search(
                    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                    r.text, _re.DOTALL
                )
                if not m:
                    # fallback: any script with "sku" and "price"
                    for s in _re.findall(r'<script[^>]*>(.*?)</script>', r.text, _re.DOTALL):
                        if '"sku"' in s and '"price"' in s:
                            m = type('M', (), {'group': lambda self, n: s})()
                            break
                if not m:
                    continue

                try:
                    data = _json.loads(m.group(1))
                except:
                    continue

                # Handle @graph array
                if isinstance(data, list):
                    data = next((d for d in data if d.get('@type') == 'Product'), data[0] if data else {})
                elif data.get('@graph'):
                    data = next((d for d in data['@graph'] if d.get('@type') == 'Product'), {})

                if data.get('@type') != 'Product':
                    continue

                title    = data.get('name', '').strip()
                sku      = data.get('sku', '') or data.get('productID', '') or slug
                url      = data.get('url', f"{base_url}/products/{slug}")
                images   = data.get('image', [])
                image    = images[0] if isinstance(images, list) and images else (images if isinstance(images, str) else '')

                # Price from offers
                offers   = data.get('offers', {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price    = safe_float(str(offers.get('price', 0)))
                currency = offers.get('priceCurrency', 'USD')
                avail    = 'In stock' if 'InStock' in str(offers.get('availability', '')) else 'Out of stock'

                # Category from description
                desc     = data.get('description', '')
                category = ''
                cat_m    = _re.search(r'(?:Dealer|Category|Series)\s*:\s*([^\n<]+)', desc)
                if cat_m:
                    category = cat_m.group(1).strip()

                item_n = extract_item_number(title) or extract_item_number(sku) or sku

                out[item_n] = {
                    'item_number': item_n,
                    'title':       title,
                    'theme':       category,
                    'category':    category,
                    'brand':       store_name,
                    'image_url':   image,
                    'compare_at':  0.0,
                    'stores': {
                        store_name: StoreOffer(
                            price=price,
                            availability=avail,
                            link=url,
                            discount_pct=0.0,
                        )
                    },
                }
            except Exception as e:
                print(f"[{store_name}] slug={slug} error: {e}")

    print(f"[{store_name}] scraped {len(out)} products (pages {start_page}–{last_page_scraped})")

    # Save last page to DB meta
    if db_path and last_page_scraped > 0:
        try:
            import sqlite3 as _sq
            conn = _sq.connect(db_path)
            conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)",
                         (meta_key, str(last_page_scraped)))
            conn.commit()
            conn.close()
        except: pass

    return out


# ── Mould King Official (WooCommerce) ─────────────────────────────────────────

def fetch_mouldking(db_path: str = None) -> Dict[str, Dict[str, Any]]:
    """
    Scrape Mould King WooCommerce store via public REST API.
    Uses rotating pagination — each call starts where the last left off,
    so the full catalog is refreshed incrementally across multiple runs.
    """
    out: Dict[str, Dict[str, Any]] = {}
    store_name = "Mould King"
    PAGE_BATCH = 20   # 2000 products per run

    # Determine start page from meta
    start_page = 1
    if db_path:
        try:
            from core.db import meta_get, meta_set
            last = meta_get(db_path, "mould_king_last_page")
            if last:
                start_page = int(last) + 1
        except Exception:
            pass

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        page = start_page
        pages_done = 0
        reset = False

        while pages_done < PAGE_BATCH:
            try:
                r = client.get(
                    "https://mouldking.store/wp-json/wc/store/v1/products",
                    params={"per_page": 100, "page": page},
                )
                if r.status_code != 200:
                    print(f"[Mould King] API {r.status_code} on page {page} — resetting to page 1")
                    reset = True
                    break
                products = r.json()
                if not products:
                    print(f"[Mould King] reached end at page {page} — resetting to page 1")
                    reset = True
                    break

                for prod in products:
                    item_number = str(prod.get("sku") or prod.get("id") or "").strip()
                    if not item_number:
                        item_number = f"MK-{prod.get('id')}"

                    title      = re.sub(r"<[^>]+>", "", (prod.get("name") or "")).strip()
                    prices     = prod.get("prices") or {}
                    price_raw  = safe_float(str(prices.get("price") or "").replace(",", ""))
                    price      = price_raw / 100 if price_raw else None
                    reg_raw    = safe_float(str(prices.get("regular_price") or "").replace(",", ""))
                    compare_at = reg_raw / 100 if reg_raw else None

                    images     = prod.get("images") or []
                    image_url  = images[0].get("src") or "" if images else ""
                    link       = prod.get("permalink") or f"https://mouldking.store/?p={prod.get('id')}"
                    cats       = [c.get("name") or "" for c in (prod.get("categories") or [])]
                    theme      = cats[0] if cats else "Mould King"
                    avail      = "In stock" if prod.get("is_in_stock", True) else "Out of stock"

                    out[item_number] = {
                        "item_number": item_number,
                        "title":       title,
                        "theme":       theme,
                        "category":    "Mould King",
                        "image_url":   image_url,
                        "image_list":  [image_url] if image_url else [],
                        "vendor":      "Mould King",
                        "brand":       "Mould King",
                        "compare_at":  compare_at,
                        "is_new":      False,
                        "stores": {
                            store_name: StoreOffer(
                                price=price,
                                availability=avail,
                                link=link,
                                discount_pct=compute_discount_pct(compare_at, price),
                            )
                        },
                    }

                print(f"[Mould King] page {page}: {len(products)} products")
                pages_done += 1
                page += 1

            except Exception as e:
                print(f"[Mould King] error page {page}: {e}")
                break

    # Save last page to meta so next run continues from here
    if db_path:
        try:
            from core.db import meta_set
            meta_set(db_path, "mould_king_last_page", str(0 if reset else page - 1))
        except Exception:
            pass

    print(f"[Mould King] scraped {len(out)} products (pages {start_page}–{page-1})")
    return out


# ── LEGO.com ───────────────────────────────────────────────────────────────────

LEGO_COM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":    "https://www.lego.com/en-us/categories/new-sets",
}

# LEGO.com product API endpoint
LEGO_COM_API = "https://www.lego.com/api/4.0/en-US/products/search"


def fetch_lego_com(db_path: str = None) -> Dict[str, Dict[str, Any]]:
    """
    Fetch LEGO Official catalog via Brickset API v3.
    Returns sets with US retail price from LEGOCom.US.retailPrice.
    Requires API_KEYS["brickset"] in config.py.
    Uses rotating year pagination to avoid hammering the API.
    """
    import httpx as _httpx
    from datetime import datetime as _dt
    from modules.brickradar.config import API_KEYS

    api_key = API_KEYS.get("brickset", "").strip()
    if not api_key:
        print("[LEGO Official] No Brickset API key — set API_KEYS['brickset'] in config.py")
        return {}

    store_name  = "LEGO Official"
    meta_key    = "lego_brickset_last_year"
    current_year = _dt.now().year

    # Rotating year — scrape one year per run
    start_year = current_year
    if db_path:
        try:
            import sqlite3 as _sq
            conn = _sq.connect(db_path)
            row  = conn.execute("SELECT v FROM meta WHERE k=?", (meta_key,)).fetchone()
            conn.close()
            if row:
                last = int(row[0])
                start_year = last - 1 if last > current_year - 5 else current_year
        except: pass

    out: Dict[str, Dict[str, Any]] = {}
    base = "https://brickset.com/api/v3.asmx/getSets"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    print(f"[LEGO Official] fetching sets year={start_year} via Brickset API...")

    page = 1
    while True:
        params = {
            "apiKey":   api_key,
            "userHash": "",
            "params":   f'{{"year":"{start_year}","pageSize":500,"pageNumber":{page},"orderBy":"Number"}}',
        }
        try:
            import httpx as _httpx
            r = _httpx.post(base, data=params, headers=headers, timeout=30)
            if r.status_code != 200:
                print(f"[LEGO Official] API error: {r.status_code}")
                break
            data = r.json()
            sets = data.get("sets", [])
            if not sets:
                break

            for s in sets:
                # Skip non-retail / no price
                lego_com  = s.get("LEGOCom", {}) or {}
                us        = lego_com.get("US", {}) or {}
                price     = us.get("retailPrice")
                if not price:
                    continue

                number    = s.get("number", "")
                variant   = s.get("numberVariant", 1)
                item_n    = f"{number}-{variant}" if variant and variant != 1 else number
                title     = s.get("name", "").strip()
                theme     = s.get("theme", "")
                subtheme  = s.get("subtheme", "")
                image     = (s.get("image") or {}).get("imageURL", "")
                url       = s.get("bricksetURL", f"https://brickset.com/sets/{item_n}")
                avail     = s.get("availability", "")
                in_stock  = avail.lower() not in ("discontinued", "retired", "") if avail else True

                out[item_n] = {
                    "item_number": item_n,
                    "title":       title,
                    "theme":       theme,
                    "category":    subtheme or theme,
                    "brand":       "LEGO",
                    "image_url":   image,
                    "compare_at":  0.0,
                    "stores": {
                        store_name: StoreOffer(
                            price=float(price),
                            availability="In stock" if in_stock else "Out of stock",
                            link=url,
                            discount_pct=0.0,
                        )
                    },
                }

            # Check if more pages
            total   = data.get("matches", 0)
            fetched = page * 500
            if fetched >= total:
                break
            page += 1

        except Exception as e:
            print(f"[LEGO Official] error: {e}")
            break

    print(f"[LEGO Official] scraped {len(out)} sets (year={start_year})")

    # Save year to meta
    if db_path and out:
        try:
            import sqlite3 as _sq
            conn = _sq.connect(db_path)
            conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)",
                         (meta_key, str(start_year)))
            conn.commit()
            conn.close()
        except: pass

    return out


def _fetch_lego_com_html(
    client: httpx.Client,
    store_name: str,
    max_products: int,
) -> Dict[str, Dict[str, Any]]:
    """HTML fallback scraper for lego.com."""
    out: Dict[str, Dict[str, Any]] = {}

    # Key category pages
    LEGO_CATEGORY_URLS = [
        "https://www.lego.com/en-us/categories/new-sets",
        "https://www.lego.com/en-us/categories/bestsellers",
        "https://www.lego.com/en-us/themes/technic",
        "https://www.lego.com/en-us/themes/star-wars",
        "https://www.lego.com/en-us/themes/city",
        "https://www.lego.com/en-us/themes/icons",
        "https://www.lego.com/en-us/themes/creator-3-in-1",
        "https://www.lego.com/en-us/themes/speed-champions",
        "https://www.lego.com/en-us/themes/harry-potter",
        "https://www.lego.com/en-us/themes/friends",
    ]

    for cat_url in LEGO_CATEGORY_URLS:
        if len(out) >= max_products:
            break
        try:
            r = client.get(cat_url)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "lxml")

            # LEGO.com embeds product data in __NEXT_DATA__ JSON
            script = soup.select_one("#__NEXT_DATA__")
            if not script:
                continue

            data       = json.loads(script.string)
            page_props = data.get("props", {}).get("pageProps", {})

            # Navigate to product list — structure varies by page type
            products = (
                page_props.get("products") or
                page_props.get("initialData", {}).get("products") or
                []
            )

            for prod in products:
                item_number = str(prod.get("productCode") or prod.get("itemNumber") or "").strip()
                if not item_number or item_number in out:
                    continue

                title      = (prod.get("name") or "").strip()
                theme      = (prod.get("themeName") or "").strip()
                price_info = prod.get("price") or {}
                price      = safe_float(
                    str(price_info.get("formattedAmount") or price_info.get("amount") or "")
                    .replace("$", "").replace(",", "")
                )

                images    = prod.get("images") or []
                image_url = ""
                if images:
                    src = images[0].get("url") or images[0].get("src") or ""
                    image_url = ("https:" + src) if src.startswith("//") else src

                slug = prod.get("slug") or item_number
                link = f"https://www.lego.com/en-us/product/{slug}"

                out[item_number] = {
                    "item_number": item_number,
                    "title":       title,
                    "theme":       theme,
                    "category":    "LEGO",
                    "image_url":   image_url,
                    "image_list":  [image_url] if image_url else [],
                    "vendor":      "LEGO",
                    "brand":       "LEGO",
                    "compare_at":  None,
                    "is_new":      False,
                    "stores": {
                        store_name: StoreOffer(
                            price=price,
                            availability="N/A",
                            link=link,
                            discount_pct=None,
                        )
                    },
                }

        except Exception as e:
            print(f"[LEGO Official] error on {cat_url}: {e}")

    return out


# ── Bricklink ──────────────────────────────────────────────────────────────────

def fetch_bricklink_prices(set_numbers: list) -> Dict[str, Dict[str, Any]]:
    """
    Fetch new/sealed prices from Bricklink for a list of set numbers.
    Uses Bricklink's price guide API (no auth needed for basic lookups).
    Only fetches sets that are already in the local catalog to avoid noise.
    """
    out: Dict[str, Dict[str, Any]] = {}
    store_name = "Bricklink"

    with httpx.Client(
        timeout=20,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
    ) as client:
        for set_number in set_numbers[:200]:   # cap at 200 to avoid hammering
            # Bricklink uses format like "10497-1" for sets
            bl_id = f"{set_number}-1"
            url   = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={bl_id}"
            try:
                r = client.get(url)
                if r.status_code != 200:
                    continue

                soup = BeautifulSoup(r.text, "lxml")

                # Extract new/sealed price from the price guide table
                # Bricklink shows "Avg Price" in the catalog page
                price     = None
                price_els = soup.select("td.pcipgSold span")
                for el in price_els:
                    text = el.get_text(strip=True).replace("$", "").replace(",", "")
                    p    = safe_float(text)
                    if p and p > 0:
                        price = p
                        break

                title_el = soup.select_one("h1.fn")
                title    = title_el.get_text(strip=True) if title_el else ""

                image_el  = soup.select_one("#_idimgMainPic")
                image_url = ""
                if image_el:
                    src = image_el.get("src") or ""
                    image_url = ("https:" + src) if src.startswith("//") else src

                link = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={bl_id}"

                if price:
                    out[set_number] = {
                        "item_number": set_number,
                        "title":       title or f"LEGO Set {set_number}",
                        "theme":       "",
                        "category":    "LEGO",
                        "image_url":   image_url,
                        "image_list":  [image_url] if image_url else [],
                        "vendor":      "Bricklink",
                        "brand":       "LEGO",
                        "compare_at":  None,
                        "is_new":      False,
                        "stores": {
                            store_name: StoreOffer(
                                price=price,
                                availability="In stock",
                                link=link,
                                discount_pct=None,
                            )
                        },
                    }

            except Exception as e:
                print(f"[Bricklink] error for {set_number}: {e}")
                continue

    print(f"[Bricklink] fetched prices for {len(out)} sets")
    return out


# ── Official store dispatcher ──────────────────────────────────────────────────

def fetch_store_by_platform(name: str, base_url: str, platform: str,
                              collection_slug: str = "", db_path: str = None,
                              vat_multiplier: float = 1.0, lego_only: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Generic scraper dispatcher — routes to the right scraper based on platform.
    Works for ANY store in DB — no hardcoding per store.
    """
    platform = (platform or "").lower().strip()

    if platform == "shopify":
        return fetch_shopify_store(
            store_name=name,
            base_url=base_url,
            vat_multiplier=vat_multiplier,
            collection_slug=collection_slug or "",
            lego_only=lego_only,
            normalize_theme_fn=None,
        )
    elif platform == "woocommerce":
        if name == "Mould King":
            return fetch_mouldking(db_path)
        return _fetch_woocommerce_generic(name, base_url)
    elif platform == "ueeshop":
        return fetch_ueeshop_store(name, base_url, db_path=db_path)
    elif platform == "lego_com":
        return fetch_lego_com(db_path=db_path)
    else:
        print(f"[{name}] platform '{platform}' has no scraper yet")
        return {}


def _fetch_woocommerce_generic(name: str, base_url: str) -> Dict[str, Dict[str, Any]]:
    """Generic WooCommerce scraper via wp-json/wc/store/v1/products."""
    import httpx
    from core.utils import safe_float, extract_item_number, compute_discount_pct

    out: Dict[str, Dict[str, Any]] = {}
    base_url = base_url.rstrip("/")
    headers  = {"User-Agent": "Mozilla/5.0"}

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for page in range(1, 100):
            url = f"{base_url}/wp-json/wc/store/v1/products?per_page=100&page={page}&status=publish"
            try:
                r = client.get(url)
                if r.status_code != 200:
                    break
                items = r.json()
                if not items:
                    break
                for p in items:
                    title  = (p.get("name") or "").strip()
                    sku    = (p.get("sku") or "").strip()
                    item_n = sku or extract_item_number(title) or str(p.get("id", ""))
                    price  = safe_float(p.get("prices", {}).get("price", 0)) / 100
                    old_p  = safe_float(p.get("prices", {}).get("regular_price", 0)) / 100
                    link   = p.get("permalink") or ""
                    image  = (p.get("images") or [{}])[0].get("src", "")
                    avail  = "In stock" if p.get("is_in_stock") else "Out of stock"
                    disc   = compute_discount_pct(old_p, price)
                    out[item_n] = {
                        "item_number": item_n,
                        "title": title,
                        "theme": (p.get("categories") or [{}])[0].get("name", ""),
                        "category": (p.get("categories") or [{}])[0].get("name", ""),
                        "brand": name,
                        "image_url": image,
                        "compare_at": old_p,
                        "stores": {
                            name: StoreOffer(price=price, availability=avail, link=link, discount_pct=disc)
                        },
                    }
            except Exception as e:
                print(f"[{name}] page {page} error: {e}")
                break
    print(f"[{name}] scraped {len(out)} products")
    return out


def fetch_official_stores(db_path: str = None, store_filter: str = None, progress_fn=None) -> list:
    """
    Generic DB-driven official store scraper.
    Reads all enabled official stores from DB, routes each by platform.
    store_filter: scrape only this store name (None = all).
    """
    catalogs = []
    if db_path:
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur  = conn.cursor()
            cur.execute("SELECT name, base_url, platform, collection_slug, vat_multiplier, lego_only FROM stores WHERE source_type='official' AND enabled=1")
            db_stores = [dict(r) for r in cur.fetchall()]
            conn.close()
        except Exception as e:
            print(f"[fetch_official_stores] DB error: {e}")
            db_stores = []
    else:
        db_stores = [
            {"name": "CaDA Official",  "base_url": "https://www.cada-official.com", "platform": "shopify",     "collection_slug": "", "vat_multiplier": 1.0, "lego_only": False},
            {"name": "Mould King",     "base_url": "https://www.mouldkingblock.com","platform": "woocommerce", "collection_slug": "", "vat_multiplier": 1.0, "lego_only": False},
            {"name": "Reobrix Official","base_url": "https://www.reobrix.com",       "platform": "ueeshop",     "collection_slug": "", "vat_multiplier": 1.0, "lego_only": False},
        ]

    for s in db_stores:
        name = s["name"]
        if store_filter and name != store_filter:
            continue
        if progress_fn:
            progress_fn(f"Scraping {name}…")
        try:
            catalog = fetch_store_by_platform(
                name=name,
                base_url=s["base_url"],
                platform=s["platform"],
                collection_slug=s.get("collection_slug") or "",
                db_path=db_path,
                vat_multiplier=float(s.get("vat_multiplier") or 1.0),
                lego_only=bool(s.get("lego_only")),
            )
            if catalog:
                catalogs.append(catalog)
        except Exception as e:
            print(f"[{name}] ERROR: {e}")
    return catalogs
