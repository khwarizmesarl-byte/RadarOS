import re
import time
import random
import httpx
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional

from core.utils import safe_float, extract_item_number, compute_discount_pct
from core.models import StoreOffer

DEFAULT_TIMEOUT = 30.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}


def fetch_bigcommerce_store(
    store_name: str,
    base_url: str,
    category_slug: str = "",
    lego_only: bool = False,
    vat_multiplier: float = 1.0,
) -> Dict[str, Dict[str, Any]]:
    """Generic BigCommerce HTML scraper — works for any BigCommerce store."""
    out: Dict[str, Dict[str, Any]] = {}
    base     = base_url.rstrip("/")
    cat_path = ("/" + category_slug.strip("/") + "/") if category_slug else "/"

    try:
        import cloudscraper as _cs
        scraper = _cs.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        def _get(url: str):
            return scraper.get(url, timeout=20)
    except ImportError:
        print(f"[{store_name}] cloudscraper not installed — falling back to httpx")
        def _get(url: str):
            try:
                return httpx.get(url, timeout=20, follow_redirects=True, headers=HEADERS)
            except Exception as e:
                print(f"[{store_name}] fetch error: {e}")
                return None

    page = 1
    while True:
        url = f"{base}{cat_path}" if page == 1 else f"{base}{cat_path}?page={page}"
        r   = _get(url)
        if not r or r.status_code != 200:
            if r:
                print(f"[{store_name}] HTTP {r.status_code} on page {page}")
            break

        soup  = BeautifulSoup(r.text, "lxml")
        cards = (
            soup.select("article.product") or
            soup.select("li[data-product-id]") or
            soup.select(".productGrid .product") or
            soup.select("[class*='productCard']")
        )

        if not cards:
            print(f"[{store_name}] no cards on page {page} — stopping")
            break

        found_new = False
        for card in cards:
            title_el = (
                card.select_one("h4.card-title a") or
                card.select_one(".product-title a") or
                card.select_one("h3 a") or
                card.select_one("a[data-product-id]") or
                card.select_one("a")
            )
            title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
            link  = title_el.get("href", "") if title_el else ""
            if link and not link.startswith("http"):
                link = base + link
            if not title:
                continue
            if lego_only and "lego" not in title.lower():
                continue

            item_number = extract_item_number(title)
            if not item_number and link:
                item_number = extract_item_number(link.rstrip("/").split("/")[-1])
            if not item_number:
                pid_el = card.select_one("[data-product-id]")
                pid    = pid_el.get("data-product-id") if pid_el else card.get("data-product-id")
                if pid:
                    item_number = f"BC{pid}"
            if not item_number or item_number in out:
                continue
            found_new = True

            price_el = (
                card.select_one(".price--withoutTax") or
                card.select_one(".price--withTax") or
                card.select_one(".price") or
                card.select_one("[data-product-price]")
            )
            price = None
            if price_el:
                raw = safe_float(re.sub(r"[^\d.]", "", price_el.get_text(strip=True)))
                price = round(raw * vat_multiplier, 2) if raw is not None else None

            compare_el = (
                card.select_one(".price--rrp") or
                card.select_one(".price--non-sale") or
                card.select_one(".price-was")
            )
            compare_at = None
            if compare_el:
                raw_c = safe_float(re.sub(r"[^\d.]", "", compare_el.get_text(strip=True)))
                compare_at = round(raw_c * vat_multiplier, 2) if raw_c is not None else None

            img_el    = (
                card.select_one("img.card-image") or
                card.select_one("img[data-src]") or
                card.select_one("img")
            )
            image_url = ""
            if img_el:
                image_url = (img_el.get("data-src") or img_el.get("src") or "").strip()
                if image_url.startswith("//"):
                    image_url = "https:" + image_url

            availability = "In stock"
            avail_el = card.select_one(".stock-level, [data-in-stock]")
            if avail_el and "out" in avail_el.get_text(strip=True).lower():
                availability = "Out of stock"

            out[item_number] = {
                "item_number": item_number,
                "title":       title,
                "theme":       "",
                "category":    "LEGO" if lego_only else "",
                "image_url":   image_url,
                "image_list":  [image_url] if image_url else [],
                "vendor":      store_name,
                "brand":       "LEGO" if lego_only else "UNKNOWN",
                "compare_at":  compare_at,
                "is_new":      False,
                "stores": {
                    store_name: StoreOffer(
                        price=price,
                        availability=availability,
                        link=link or f"{base}{cat_path}",
                        discount_pct=compute_discount_pct(price, compare_at),
                        stock_qty=None,
                    )
                },
            }

        print(f"[{store_name}] page {page}: {len(cards)} cards, {len(out)} total")
        if not found_new:
            break

        next_el = soup.select_one(
            "a.pagination-item--next, .pagination a[rel='next'], a[aria-label='Next page']"
        )
        if not next_el and len(cards) < 12:
            break

        page += 1
        time.sleep(random.uniform(2.0, 4.0))

    print(f"[{store_name}] total scraped: {len(out)}")
    return out
