import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from core.db import (
    db_connect, persist_snapshot, compute_alerts,
    get_db_stores, meta_set
)
from core.utils import utc_now_iso, extract_item_number


# ── Merge key ──────────────────────────────────────────────────────────────────

def make_merge_key(item_number: str, title: str = "") -> str:
    """
    Canonical key used to merge the same product across stores.
    Strips brand prefix so CADA-10345 and 10345 resolve to the same key.
    """
    clean = re.sub(r"^[A-Z]+-", "", (item_number or "").strip())
    return clean if clean else item_number


# ── Catalog merge ──────────────────────────────────────────────────────────────

def merge_catalogs(catalogs: List[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    Merge per-store catalogs into a single unified catalog.
    Same item_number → merged stores dict.
    Best title/image/theme kept from whichever store has the richest data.
    """
    merged: Dict[str, Dict[str, Any]] = {}

    for catalog in catalogs:
        for item_number, rec in catalog.items():
            key = make_merge_key(item_number)

            if key not in merged:
                merged[key] = {
                    "item_number": key,
                    "title":       rec.get("title") or "",
                    "theme":       rec.get("theme") or "",
                    "category":    rec.get("category") or "",
                    "image_url":   rec.get("image_url") or "",
                    "image_list":  rec.get("image_list") or [],
                    "brand":       rec.get("brand") or "",
                    "compare_at":  rec.get("compare_at"),
                    "is_new":      rec.get("is_new", False),
                    "stores":      {},
                }

            existing = merged[key]

            # Keep richer metadata
            if not existing["title"] and rec.get("title"):
                existing["title"] = rec["title"]
            if not existing["image_url"] and rec.get("image_url"):
                existing["image_url"] = rec["image_url"]
                existing["image_list"] = rec.get("image_list") or []
            if not existing["theme"] and rec.get("theme"):
                existing["theme"] = rec["theme"]
            if not existing["category"] and rec.get("category"):
                existing["category"] = rec["category"]
            if not existing["brand"] and rec.get("brand"):
                existing["brand"] = rec["brand"]
            if rec.get("is_new"):
                existing["is_new"] = True

            # compare_at: keep lowest (most conservative discount baseline)
            new_ca = rec.get("compare_at")
            if new_ca is not None:
                if existing["compare_at"] is None or new_ca < existing["compare_at"]:
                    existing["compare_at"] = new_ca

            # Merge store offers
            for store_name, offer in (rec.get("stores") or {}).items():
                existing["stores"][store_name] = offer

    return merged


# ── Store config helpers ───────────────────────────────────────────────────────

def build_store_configs(
    hardcoded_shopify: Dict,
    hardcoded_bigcommerce: Dict,
    db_path: str,
) -> tuple:
    """
    Merge hardcoded store configs with DB-managed stores.
    Returns (shopify_stores, bigcommerce_stores, html_stores).
    """
    shopify     = dict(hardcoded_shopify)
    bigcommerce = dict(hardcoded_bigcommerce)
    html_stores = []

    try:
        for row in get_db_stores(db_path):
            name     = row["name"]
            platform = (row["platform"] or "shopify").lower()
            cfg = {
                "url":             row["base_url"],
                "vat_multiplier":  row["vat_multiplier"] or 1.0,
                "collection_slug": row["collection_slug"] or "",
                "new_arrivals_collection": row["new_arrivals_collection"] or "",
                "lego_only":       bool(row["lego_only"]) if "lego_only" in row.keys() else False,
            }
            if platform == "shopify":
                shopify[name] = cfg
            elif platform in ("bigcommerce", "bigc"):
                bigcommerce[name] = cfg
            elif platform == "html":
                html_stores.append((name, cfg))
    except Exception as e:
        print(f"[engine] DB store load error: {e}")

    return shopify, bigcommerce, html_stores


# ── Refresh orchestration ──────────────────────────────────────────────────────

def refresh_all(
    db_path: str,
    shopify_stores: Dict,
    bigcommerce_stores: Dict,
    html_stores: List,
    fetch_shopify_fn: Callable,
    fetch_bigcommerce_fn: Callable,
    fetch_html_stores_fn: Optional[Callable],
    progress_fn: Optional[Callable] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Orchestrate a full scrape of all stores.

    progress_fn(message: str) — optional SSE / log callback.

    fetch_shopify_fn    — core.scrapers.shopify.fetch_shopify_store (or module override)
    fetch_bigcommerce_fn — core.scrapers.bigcommerce.fetch_bigcommerce_store
    fetch_html_stores_fn — module-specific callable that returns list of catalogs,
                           or None if module has no HTML stores.
    """
    def _log(msg: str):
        print(msg)
        if progress_fn:
            progress_fn(msg)

    captured_at = utc_now_iso()
    catalogs: List[Dict] = []

    total = len(shopify_stores) + len(bigcommerce_stores) + len(html_stores)
    done  = 0

    # ── Shopify stores ──────────────────────────────────────────────────────
    for store_name, cfg in shopify_stores.items():
        _log(f"Scraping {store_name} (Shopify)…")
        try:
            from core.scrapers.shopify import fetch_new_arrival_items
            new_items = set()
            na_slug = cfg.get("new_arrivals_collection") or ""
            if na_slug:
                new_items = fetch_new_arrival_items(store_name, cfg["url"], na_slug)

            catalog = fetch_shopify_fn(
                store_name=store_name,
                base_url=cfg["url"],
                vat_multiplier=cfg.get("vat_multiplier", 1.0),
                new_items=new_items,
                collection_slug=cfg.get("collection_slug") or "",
                lego_only=cfg.get("lego_only", False),
                normalize_theme_fn=cfg.get("normalize_theme_fn"),
            )
            catalogs.append(catalog)
            _update_store_meta(db_path, store_name, len(catalog), captured_at)
        except Exception as e:
            _log(f"[{store_name}] ERROR: {e}")

        done += 1
        _log(f"Progress: {done}/{total}")

    # ── BigCommerce stores ──────────────────────────────────────────────────
    for store_name, cfg in bigcommerce_stores.items():
        _log(f"Scraping {store_name} (BigCommerce)…")
        try:
            catalog = fetch_bigcommerce_fn(
                store_name=store_name,
                base_url=cfg["url"],
                category_slug=cfg.get("collection_slug") or "",
                lego_only=cfg.get("lego_only", False),
                vat_multiplier=cfg.get("vat_multiplier", 1.0),
            )
            catalogs.append(catalog)
            _update_store_meta(db_path, store_name, len(catalog), captured_at)
        except Exception as e:
            _log(f"[{store_name}] ERROR: {e}")

        done += 1
        _log(f"Progress: {done}/{total}")

    # ── HTML stores (module-specific) ───────────────────────────────────────
    if fetch_html_stores_fn and html_stores:
        _log("Scraping HTML stores…")
        try:
            html_catalogs = fetch_html_stores_fn(html_stores, progress_fn=_log)
            catalogs.extend(html_catalogs)
        except Exception as e:
            _log(f"[html_stores] ERROR: {e}")
        done += len(html_stores)
        _log(f"Progress: {done}/{total}")

    # ── Merge & persist ─────────────────────────────────────────────────────
    _log("Merging catalogs…")
    merged = merge_catalogs(catalogs)

    _log("Persisting snapshots…")
    for store_name in list(shopify_stores) + list(bigcommerce_stores):
        persist_snapshot(db_path, captured_at, store_name, merged)

    _log("Computing alerts…")
    for store_name in list(shopify_stores) + list(bigcommerce_stores):
        compute_alerts(db_path, captured_at, store_name, merged)

    meta_set(db_path, "last_refresh", captured_at)
    _log(f"Refresh complete — {len(merged)} products at {captured_at}")

    return merged


# ── Internal helpers ───────────────────────────────────────────────────────────

def _update_store_meta(db_path: str, store_name: str, product_count: int, scraped_at: str) -> None:
    try:
        conn = db_connect(db_path)
        conn.execute(
            "UPDATE stores SET product_count=?, last_scraped=? WHERE name=?",
            (product_count, scraped_at, store_name)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
