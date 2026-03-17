import re
import httpx
from bs4 import BeautifulSoup
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.utils import safe_float, extract_item_number, compute_discount_pct
from core.models import StoreOffer

DEFAULT_TIMEOUT = 30.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}


def fetch_html_store(
    store_name: str,
    listing_urls: List[str],
    base_url: str,
    parse_card_fn: Callable,
    use_cloudscraper: bool = False,
    max_pages: int = 50,
    page_url_fn: Optional[Callable] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Generic paginated HTML scraper.

    parse_card_fn(card, base_url) -> dict or None
        Must return a dict with keys:
            item_number, title, theme, category, image_url,
            brand, compare_at, is_new, price, availability, link

    page_url_fn(base, page) -> str
        Optional — how to construct page 2, 3 … URLs.
        Defaults to appending /page-{n}/ to base.
    """
    out: Dict[str, Dict[str, Any]] = {}

    if use_cloudscraper:
        try:
            import cloudscraper
            client = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            def _get(url):
                return client.get(url, timeout=DEFAULT_TIMEOUT)
        except ImportError:
            print(f"[{store_name}] cloudscraper not installed — falling back to httpx")
            use_cloudscraper = False

    if not use_cloudscraper:
        _client = httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=HEADERS)
        def _get(url):
            return _client.get(url)

    def _default_page_url(base: str, page: int) -> str:
        return base.rstrip("/") + (f"/page-{page}/" if page > 1 else "/")

    _page_url = page_url_fn or _default_page_url

    for base in listing_urls:
        for page in range(1, max_pages + 1):
            url = _page_url(base, page)
            try:
                r = _get(url)
            except Exception as e:
                print(f"[{store_name}] fetch error: {e}")
                break
            if r.status_code != 200:
                print(f"[{store_name}] HTTP {r.status_code} for {url}")
                break

            soup  = BeautifulSoup(r.text, "lxml")
            cards = soup.select("div.thumbnail.grid-thumbnail")

            # fallback selectors for other HTML stores
            if not cards:
                cards = (
                    soup.select(".product-card") or
                    soup.select("li.product") or
                    soup.select("[class*='product-item']")
                )

            if not cards:
                print(f"[{store_name}] page {page}: 0 cards — stopping")
                break

            found_any = False
            for card in cards:
                try:
                    rec = parse_card_fn(card, base_url)
                except Exception as e:
                    print(f"[{store_name}] parse error: {e}")
                    continue
                if not rec or not rec.get("item_number"):
                    continue

                found_any = True
                item_number = rec["item_number"]
                price       = rec.get("price")
                availability = rec.get("availability", "In stock" if price else "N/A")
                link        = rec.get("link", base_url)
                image_url   = rec.get("image_url", "")
                discount_pct = compute_discount_pct(price, rec.get("compare_at"))

                out[item_number] = {
                    "item_number": item_number,
                    "title":       rec.get("title", ""),
                    "theme":       rec.get("theme", ""),
                    "category":    rec.get("category", ""),
                    "image_url":   image_url,
                    "image_list":  [image_url] if image_url else [],
                    "vendor":      store_name,
                    "brand":       rec.get("brand", "UNKNOWN"),
                    "compare_at":  rec.get("compare_at"),
                    "is_new":      rec.get("is_new", False),
                    "stores": {
                        store_name: StoreOffer(
                            price=price,
                            availability=availability,
                            link=link,
                            discount_pct=discount_pct,
                        )
                    },
                }

            print(f"[{store_name}] page {page}: {len(cards)} cards")
            if not found_any:
                break

    print(f"[{store_name}] total scraped: {len(out)}")
    return out
