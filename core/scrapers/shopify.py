import re
import time
import random
import httpx
from typing import Any, Dict, Optional, Set

from core.utils import safe_float, extract_item_number, compute_discount_pct, normalize_brand_from_vendor_title
from core.models import StoreOffer

SHOPIFY_PAGE_LIMIT = 250
DEFAULT_TIMEOUT = 30.0
SHOPIFY_429_RETRIES = 4
SHOPIFY_429_BACKOFF = [5, 10, 20, 40]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}


def _shopify_get(client: httpx.Client, url: str, store_name: str):
    for attempt, wait in enumerate([0] + SHOPIFY_429_BACKOFF):
        if wait:
            jitter = random.uniform(0, wait * 0.3)
            print(f"[{store_name}] 429 — waiting {wait:.0f}s before retry {attempt}/{SHOPIFY_429_RETRIES}")
            time.sleep(wait + jitter)
        try:
            r = client.get(url)
        except Exception as e:
            print(f"[{store_name}] fetch error: {e}")
            return None
        if r.status_code == 429:
            if attempt >= SHOPIFY_429_RETRIES:
                return r
            continue
        return r
    return None


def fetch_new_arrival_items(store_name: str, base_url: str, collection_slug: str) -> Set[str]:
    if not collection_slug:
        return set()
    new_items: Set[str] = set()
    with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=HEADERS) as client:
        page = 1
        while True:
            url = (f"{base_url.rstrip('/')}/collections/{collection_slug}"
                   f"/products.json?limit={SHOPIFY_PAGE_LIMIT}&page={page}")
            r = _shopify_get(client, url, store_name)
            if r is None or r.status_code != 200:
                break
            products = r.json().get("products") or []
            if not products:
                break
            for prod in products:
                title = (prod.get("title") or "").strip()
                item_number = extract_item_number(title)
                if not item_number:
                    for vv in (prod.get("variants") or []):
                        item_number = extract_item_number((vv.get("sku") or "").strip())
                        if item_number:
                            break
                if item_number:
                    new_items.add(item_number)
            if len(products) < SHOPIFY_PAGE_LIMIT:
                break
            page += 1
    print(f"[{store_name}/new-arrivals] found {len(new_items)} new items")
    return new_items


def fetch_shopify_store(
    store_name: str,
    base_url: str,
    vat_multiplier: float = 1.0,
    new_items: Optional[Set[str]] = None,
    collection_slug: Optional[str] = None,
    lego_only: bool = False,
    normalize_theme_fn=None,
) -> Dict[str, Dict[str, Any]]:
    """
    Generic Shopify scraper.
    normalize_theme_fn: optional callable(prod) -> (theme, category)
    """
    out: Dict[str, Dict[str, Any]] = {}

    with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=HEADERS) as client:
        page = 1
        while True:
            if collection_slug:
                url = (f"{base_url.rstrip('/')}/collections/{collection_slug}"
                       f"/products.json?limit={SHOPIFY_PAGE_LIMIT}&page={page}")
            else:
                url = f"{base_url.rstrip('/')}/products.json?limit={SHOPIFY_PAGE_LIMIT}&page={page}"

            r = _shopify_get(client, url, store_name)
            if r is None or r.status_code != 200:
                if r is not None:
                    print(f"[{store_name}] HTTP {r.status_code}")
                break

            products = r.json().get("products") or []
            if not products:
                break

            for prod in products:
                title  = (prod.get("title") or "").strip()
                handle = (prod.get("handle") or "").strip()
                link   = f"{base_url.rstrip('/')}/products/{handle}" if handle else base_url

                item_number = extract_item_number(title)
                if not item_number:
                    for vv in (prod.get("variants") or []):
                        item_number = extract_item_number((vv.get("sku") or "").strip())
                        if item_number:
                            break
                if not item_number:
                    item_number = extract_item_number(handle)
                if not item_number:
                    shopify_id = prod.get("id")
                    item_number = f"SID{shopify_id}" if shopify_id else None
                if not item_number:
                    continue

                variants = prod.get("variants") or []
                v0 = variants[0] if variants else {}

                raw_price   = safe_float(v0.get("price"))
                raw_compare = safe_float(v0.get("compare_at_price"))
                price      = round(raw_price * vat_multiplier, 2)   if raw_price   is not None else None
                compare_at = round(raw_compare * vat_multiplier, 2) if raw_compare is not None else None

                availability = "In stock" if bool(v0.get("available", True)) else "Out of stock"

                stock_qty = None
                if variants:
                    try:
                        stock_qty = max(sum(int(v.get("inventory_quantity") or 0) for v in variants), 0)
                    except Exception:
                        stock_qty = None

                images     = prod.get("images") or []
                image_list = [(img.get("src") or "").strip() for img in images[:4] if img.get("src")]
                image_url  = image_list[0] if image_list else ""

                if normalize_theme_fn:
                    theme, category = normalize_theme_fn(prod)
                else:
                    product_type = (prod.get("product_type") or "").strip()
                    theme    = product_type
                    category = product_type.split()[0] if product_type else ""

                vendor_string = (prod.get("vendor") or "").strip()
                brand        = normalize_brand_from_vendor_title(vendor_string, title)
                discount_pct = compute_discount_pct(price, compare_at)

                if lego_only and brand != "LEGO":
                    v_check = vendor_string.split("/")[0].strip()
                    if re.sub(r"[®\s]", "", v_check).upper() == "LEGO":
                        brand = "LEGO"
                    else:
                        continue

                is_new = bool(new_items and item_number in new_items)

                if brand and brand.upper() != "LEGO":
                    item_number = f"{brand.upper()}-{item_number}"

                out[item_number] = {
                    "item_number": item_number,
                    "title":       title,
                    "theme":       theme,
                    "category":    category,
                    "image_url":   image_url,
                    "image_list":  image_list,
                    "vendor":      vendor_string,
                    "brand":       brand,
                    "compare_at":  compare_at,
                    "is_new":      is_new,
                    "stores": {
                        store_name: StoreOffer(
                            price=price,
                            availability=availability,
                            link=link,
                            discount_pct=discount_pct,
                            stock_qty=stock_qty,
                        )
                    },
                }

            if len(products) < SHOPIFY_PAGE_LIMIT:
                break
            page += 1

    print(f"[{store_name}] scraped {len(out)} items (lego_only={lego_only})")
    return out
