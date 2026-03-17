"""
BrickRadar-specific scrapers.
fetch_brickshop  — WooCommerce, parallel category scraping
fetch_playone    — Cloudflare-protected HTML store
fetch_html_stores — called by engine.refresh_all as fetch_html_stores_fn
"""

import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

from core.models import StoreOffer
from core.utils import safe_float, extract_item_number, compute_discount_pct
from modules.brickradar.config import (
    PLAYONE_LISTING_URLS,
    BRICKSHOP_LISTING_URL,
    LEGO_THEMES,
)

DEFAULT_TIMEOUT = 30.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}

# ── Theme normalisation ────────────────────────────────────────────────────────

def normalize_theme_category_from_shopify(prod: dict) -> tuple:
    """
    BrickRadar override for fetch_shopify_store's normalize_theme_fn.
    Returns (theme, category).
    """
    product_type = (prod.get("product_type") or "").strip()
    raw_tags     = prod.get("tags") or ""
    if isinstance(raw_tags, list):
        tags = [t.strip() for t in raw_tags]
    else:
        tags = [t.strip() for t in raw_tags.split(",")]
    title        = (prod.get("title") or "").strip()

    for candidate in [product_type] + tags:
        for theme in LEGO_THEMES:
            if theme.lower() == candidate.lower():
                return theme, "LEGO"

    for theme in LEGO_THEMES:
        if theme.lower() in title.lower():
            return theme, "LEGO"

    return product_type or "Other", "LEGO"


def normalize_theme_category_from_playone(title: str) -> tuple:
    """Infer LEGO theme from product title for PlayOne listings."""
    t = title.lower()
    for theme in LEGO_THEMES:
        if theme.lower() in t:
            return theme, "LEGO"
    return "Other", "LEGO"


# ── BRICKSHOP (WooCommerce) ────────────────────────────────────────────────────

BRICKSHOP_CATEGORIES = [
    ("architecture",              "Architecture"),
    ("art",                       "Art"),
    ("brickheadz",                "BrickHeadz"),
    ("city",                      "City"),
    ("classic",                   "Classic"),
    ("creator",                   "Creator"),
    ("dc-comics",                 "DC"),
    ("disney",                    "Disney"),
    ("dots",                      "DOTS"),
    ("dreamzzz",                  "DREAMZzz"),
    ("duplo",                     "DUPLO"),
    ("friends",                   "Friends"),
    ("gabbys-dollhouse",          "Gabby's Dollhouse"),
    ("harry-potter",              "Harry Potter"),
    ("icons",                     "Icons"),
    ("ideas",                     "Ideas"),
    ("indiana-jones",             "Indiana Jones"),
    ("jurassic-world",            "Jurassic World"),
    ("marvel",                    "Marvel"),
    ("minecraft",                 "Minecraft"),
    ("ninjago",                   "NINJAGO"),
    ("sonic-the-hedgehog",        "Sonic"),
    ("speed-champions",           "Speed Champions"),
    ("star-wars",                 "Star Wars"),
    ("super-mario",               "Super Mario"),
    ("technic",                   "Technic"),
    ("botanical-collection",      "Botanical"),
    ("modular-buildings",         "Modular Buildings"),
    ("ultimate-collector-series", "UCS"),
    ("polybags",                  "Polybags"),
    ("minifigures",               "Minifigures"),
    ("advent-calendar",           "Advent Calendar"),
]


def fetch_brickshop() -> Dict[str, Dict[str, Any]]:
    """
    Scrape BRICKSHOP (WooCommerce) by iterating all theme category pages.
    Categories scraped in parallel (8 workers).
    Item number extracted from URL slug (most reliable).
    """
    BASE = "https://brickshop.me"
    out: Dict[str, Dict[str, Any]] = {}
    seen_links: set = set()
    lock = threading.Lock()

    def _scrape_category(cat_slug: str, theme_name: str) -> None:
        local: Dict[str, Dict] = {}
        local_links: set = set()

        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=HEADERS) as client:
            for page in range(1, 50):
                url = (
                    f"{BASE}/product-category/{cat_slug}/"
                    if page == 1
                    else f"{BASE}/product-category/{cat_slug}/page/{page}/"
                )
                try:
                    r = client.get(url)
                except Exception as e:
                    print(f"[BRICKSHOP/{cat_slug}] error: {e}")
                    break

                if r.status_code == 404:
                    break
                if r.status_code != 200:
                    print(f"[BRICKSHOP/{cat_slug}] HTTP {r.status_code}")
                    break

                soup  = BeautifulSoup(r.text, "lxml")
                cards = soup.select("ul.products li.product")
                if not cards:
                    break

                found_new = False
                for card in cards:
                    a    = card.select_one("a.woocommerce-LoopProduct-link, h2 a, a[href*='/product/']")
                    link = (a.get("href") or "").strip() if a else ""
                    if not link or link in local_links:
                        continue
                    local_links.add(link)
                    found_new = True

                    title_el = card.select_one(".woocommerce-loop-product__title, h2, h3")
                    title    = (title_el.get_text(" ", strip=True) if title_el else "").strip()
                    if not title:
                        title = link.rstrip("/").split("/")[-1].replace("-", " ").title()

                    slug        = link.rstrip("/").split("/")[-1]
                    item_number = extract_item_number(slug) or extract_item_number(title)
                    if not item_number:
                        continue

                    price      = None
                    compare_at = None
                    price_el   = card.select_one(".price")
                    if price_el:
                        price_text = price_el.get_text(" ", strip=True)
                        amounts    = re.findall(r"\$\s*([0-9]+(?:\.[0-9]+)?)", price_text)
                        if len(amounts) >= 2:
                            compare_at = safe_float(amounts[0])
                            price      = safe_float(amounts[1])
                        elif len(amounts) == 1:
                            price = safe_float(amounts[0])

                    oos          = card.select_one(".out-of-stock")
                    availability = "Out of stock" if oos else ("In stock" if price else "N/A")

                    image_url  = ""
                    image_list = []
                    for img in card.select("img"):
                        src = (img.get("data-src") or img.get("src") or "").strip()
                        if src and not src.startswith("data:") and any(
                            ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]
                        ):
                            image_list.append(src)
                            if not image_url:
                                image_url = src

                    discount_pct = compute_discount_pct(price, compare_at)
                    is_new       = bool(card.select_one(".badge-new, .onsale, [class*='new']"))

                    local[item_number] = {
                        "item_number": item_number,
                        "title":       title,
                        "theme":       theme_name,
                        "category":    "LEGO",
                        "image_url":   image_url,
                        "image_list":  image_list[:4],
                        "vendor":      "BRICKSHOP",
                        "brand":       "LEGO",
                        "compare_at":  compare_at,
                        "is_new":      is_new,
                        "stores": {
                            "BRICKSHOP": StoreOffer(
                                price=price,
                                availability=availability,
                                link=link,
                                discount_pct=discount_pct,
                            )
                        },
                    }

                if not found_new:
                    break

        with lock:
            for item_number, rec in local.items():
                lnk = rec["stores"]["BRICKSHOP"].link
                if lnk not in seen_links:
                    seen_links.add(lnk)
                    out[item_number] = rec

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_scrape_category, slug, name) for slug, name in BRICKSHOP_CATEGORIES]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                print(f"[BRICKSHOP] category error: {e}")

    print(f"[BRICKSHOP] scraped {len(out)} products")
    return out


# ── PlayOne (Cloudflare HTML) ──────────────────────────────────────────────────

def fetch_playone() -> Dict[str, Dict[str, Any]]:
    """
    Scrape PlayOne — behind Cloudflare, uses cloudscraper to bypass JS challenge.
    Falls back to httpx with a warning if cloudscraper not installed.
    """
    out: Dict[str, Dict[str, Any]] = {}

    try:
        import cloudscraper
        client = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        def _get(url: str):
            return client.get(url, timeout=DEFAULT_TIMEOUT)
    except ImportError:
        print("[PlayOne] WARNING: cloudscraper not installed. Run: pip install cloudscraper")
        print("[PlayOne] Falling back to httpx (will likely fail Cloudflare check)")
        _hx = httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=HEADERS)
        def _get(url: str):
            return _hx.get(url)

    for base in PLAYONE_LISTING_URLS:
        for page in range(1, 51):
            url = base.rstrip("/") + (f"/page-{page}/" if page > 1 else "/")
            try:
                r = _get(url)
            except Exception as e:
                print(f"[PlayOne] fetch error: {e}")
                break

            if r.status_code != 200:
                print(f"[PlayOne] HTTP {r.status_code} for {url}")
                break

            soup  = BeautifulSoup(r.text, "lxml")
            cards = soup.select("div.thumbnail.grid-thumbnail")
            if not cards:
                title_tag  = soup.select_one("title")
                page_title = title_tag.get_text(strip=True) if title_tag else "N/A"
                print(f"[PlayOne] page {page}: 0 cards. Title: '{page_title}' len={len(r.text)}")
                break

            found_any = False
            for card in cards:
                a = card.select_one("a.product-title[href]")
                if not a:
                    continue

                href  = a.get("href") or ""
                if href.startswith("/"):
                    href = "https://playone.com.lb" + href

                title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
                title = title.replace("\u00a0", " ").replace("&amp;", "&").strip()
                if not title:
                    continue

                item_number = extract_item_number(title) or extract_item_number(href)
                if not item_number:
                    continue

                # Price: <span id="sec_discounted_price_NNN" class="price-num">
                price      = None
                price_span = card.select_one("[id^='sec_discounted_price_']")
                if price_span:
                    price = safe_float(price_span.get_text(strip=True).replace(",", ""))

                # Compare-at: <span id="sec_list_price_NNN" class="list-price">
                compare_at = None
                list_span  = card.select_one("[id^='sec_list_price_']")
                if list_span:
                    compare_at = safe_float(list_span.get_text(strip=True).replace(",", ""))

                discount_pct = compute_discount_pct(price, compare_at)

                image_url = ""
                img       = card.select_one("div.grid-list-image img[src]")
                if img:
                    src = img.get("src") or img.get("data-src") or ""
                    if src and not src.startswith("data:"):
                        image_url = src if src.startswith("http") else "https://playone.com.lb" + src

                is_new            = bool(card.select_one("div.new-label"))
                theme, category   = normalize_theme_category_from_playone(title)
                found_any         = True

                out[item_number] = {
                    "item_number": item_number,
                    "title":       title,
                    "theme":       theme,
                    "category":    category,
                    "image_url":   image_url,
                    "image_list":  [image_url] if image_url else [],
                    "vendor":      "PlayOne",
                    "brand":       "LEGO",
                    "compare_at":  compare_at,
                    "is_new":      is_new,
                    "stores": {
                        "PlayOne": StoreOffer(
                            price=price,
                            availability="available" if price is not None else "unavailable",
                            link=href,
                            discount_pct=discount_pct,
                        )
                    },
                }

            print(f"[PlayOne] page {page}: {len(cards)} products")
            if not found_any:
                break

    print(f"[PlayOne] total scraped: {len(out)}")
    return out


# ── HTML store dispatcher (called by engine.refresh_all) ──────────────────────

def fetch_html_stores(
    html_stores: list,
    progress_fn: Optional[Callable] = None,
) -> list:
    """
    Entry point for all BrickRadar HTML stores.
    Returns list of catalogs (one per store).
    html_stores: list of (name, cfg) tuples from DB — not used here since
                 BRICKSHOP and PlayOne are hardcoded, but kept for future DB stores.
    """
    catalogs = []

    if progress_fn:
        progress_fn("Scraping BRICKSHOP (WooCommerce)…")
    try:
        catalogs.append(fetch_brickshop())
    except Exception as e:
        print(f"[BRICKSHOP] ERROR: {e}")

    if progress_fn:
        progress_fn("Scraping PlayOne…")
    try:
        catalogs.append(fetch_playone())
    except Exception as e:
        print(f"[PlayOne] ERROR: {e}")

    return catalogs
