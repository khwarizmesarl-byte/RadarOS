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


# ── Mould King Official (WooCommerce) ─────────────────────────────────────────

def fetch_mouldking() -> Dict[str, Dict[str, Any]]:
    """Scrape Mould King WooCommerce store via public REST API."""
    out: Dict[str, Dict[str, Any]] = {}
    store_name = "Mould King"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        page = 1
        while True:
            try:
                r = client.get(
                    "https://mouldking.store/wp-json/wc/store/v1/products",
                    params={"per_page": 100, "page": page},
                )
                if r.status_code != 200:
                    print(f"[Mould King] API {r.status_code} on page {page}")
                    break
                products = r.json()
                if not products:
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
                if len(products) < 100:
                    break
                page += 1

            except Exception as e:
                print(f"[Mould King] error page {page}: {e}")
                break

    print(f"[Mould King] scraped {len(out)} products")
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


def fetch_lego_com(max_products: int = 500) -> Dict[str, Dict[str, Any]]:
    """
    Scrape LEGO.com via their product search API.
    Returns USD prices from the US store as the official MSRP reference.
    """
    out: Dict[str, Dict[str, Any]] = {}
    store_name = "LEGO Official"

    params = {
        "offset":   0,
        "limit":    24,
        "sort":     "RELEVANCE",
        "category": "",
    }

    with httpx.Client(
        timeout=30,
        follow_redirects=True,
        headers=LEGO_COM_HEADERS,
    ) as client:

        # First try the API endpoint
        try:
            r = client.get(LEGO_COM_API, params={**params, "limit": 24, "offset": 0})
            if r.status_code == 200:
                data = r.json()
                total = data.get("total", 0)
                print(f"[LEGO Official] API total: {total} products")

                all_products = data.get("results") or []

                # Paginate
                offset = 24
                while offset < min(total, max_products):
                    r2 = client.get(LEGO_COM_API, params={**params, "limit": 24, "offset": offset})
                    if r2.status_code != 200:
                        break
                    page_data = r2.json()
                    page_products = page_data.get("results") or []
                    if not page_products:
                        break
                    all_products.extend(page_products)
                    offset += 24

                for prod in all_products:
                    item_number = str(prod.get("productCode") or prod.get("itemNumber") or "").strip()
                    if not item_number:
                        continue

                    title       = (prod.get("name") or "").strip()
                    theme       = (prod.get("themeName") or prod.get("theme") or "").strip()
                    price_info  = prod.get("price") or {}
                    price       = safe_float(price_info.get("formattedAmount", "").replace("$", "").replace(",", ""))
                    if price is None:
                        price = safe_float(price_info.get("amount"))

                    images     = prod.get("images") or []
                    image_url  = ""
                    if images:
                        image_url = (images[0].get("url") or images[0].get("src") or "").strip()
                        if image_url and not image_url.startswith("http"):
                            image_url = "https:" + image_url

                    link = f"https://www.lego.com/en-us/product/{prod.get('slug') or item_number}"

                    avail_text  = (prod.get("availability") or {}).get("status") or ""
                    availability = "In stock" if "available" in avail_text.lower() else "Out of stock" if avail_text else "N/A"

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
                                availability=availability,
                                link=link,
                                discount_pct=None,
                            )
                        },
                    }

                print(f"[LEGO Official] scraped {len(out)} products via API")
                return out

        except Exception as e:
            print(f"[LEGO Official] API error: {e} — trying fallback")

        # Fallback: scrape category pages via HTML
        try:
            out = _fetch_lego_com_html(client, store_name, max_products)
        except Exception as e:
            print(f"[LEGO Official] HTML fallback error: {e}")

    print(f"[LEGO Official] total scraped: {len(out)}")
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

def fetch_official_stores(progress_fn=None) -> list:
    """
    Fetch all official brand sources (Tier 1).
    Returns list of catalogs.
    """
    catalogs = []

    for name, fn in [("CaDA Official", fetch_cada), ("Mould King", fetch_mouldking)]:
        if progress_fn:
            progress_fn(f"Scraping {name} (official)…")
        try:
            catalogs.append(fn())
        except Exception as e:
            print(f"[{name}] ERROR: {e}")

    if progress_fn:
        progress_fn("Scraping LEGO Official…")
    try:
        catalogs.append(fetch_lego_com())
    except Exception as e:
        print(f"[LEGO Official] ERROR: {e}")

    return catalogs
