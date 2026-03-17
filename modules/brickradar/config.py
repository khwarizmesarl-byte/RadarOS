"""
BrickRadar module configuration.
All LEGO-specific store configs and module identity live here.
"""

# ── Module identity ────────────────────────────────────────────────────────────

MODULE = {
    "id":           "brickradar",
    "name":         "BrickRadar",
    "slogan":       "Track. Compare. Save.",
    "accent_color": "#378ADD",
    "categories":   ["LEGO", "Cada", "Mould King", "Cobi", "MOC"],
    "match_strategy":           "set_number",
    "source_types":             ["official_brand", "marketplace", "retail"],
    "currency_display":         "USD",
    "enable_global_comparison": False,   # flip True when global sources added
}

# ── VAT ────────────────────────────────────────────────────────────────────────

BRICKING_VAT_MULTIPLIER = 1.00   # update if VAT changes

# ── Shopify stores ─────────────────────────────────────────────────────────────

# Collections to check for new/recent arrivals (store_name → slug)
NEW_ARRIVAL_COLLECTIONS = {
    "Brickmania": "new-arrivals",
    "Bricking":   "2026-releases",
}

SHOPIFY_STORES = {
    "Brickmania": {
        "url":            "https://thebrickmania.com",
        "vat_multiplier": 1.00,
        "lego_only":      False,
        "collection_slug": "",
        "new_arrivals_collection": NEW_ARRIVAL_COLLECTIONS.get("Brickmania", ""),
    },
    "Bricking": {
        "url":            "https://bricking.com",
        "vat_multiplier": BRICKING_VAT_MULTIPLIER,
        "lego_only":      False,
        "collection_slug": "",
        "new_arrivals_collection": NEW_ARRIVAL_COLLECTIONS.get("Bricking", ""),
    },
    "KLAPTAP": {
        "url":            "https://klaptap.com",
        "vat_multiplier": 1.00,
        "lego_only":      True,
        "collection_slug": "",
        "new_arrivals_collection": "",
    },
}

# ── BigCommerce stores ─────────────────────────────────────────────────────────

BIGCOMMERCE_STORES = {
    "Ayoub Computers": {
        "url":            "https://ayoubcomputers.com",
        "collection_slug": "lego",
        "lego_only":      True,
        "vat_multiplier": 1.0,
    },
}

# ── HTML stores (custom scrapers) ──────────────────────────────────────────────

# PlayOne: Lebanese store, uses Cloudflare — scraped via cloudscraper
PLAYONE_LISTING_URLS = [
    "https://playone.com.lb/brands/lego/",
]

# BRICKSHOP: WooCommerce store
BRICKSHOP_LISTING_URL = "https://brickshop.me/shop/"

# ── Preferred display order (fallback: alphabetical) ──────────────────────────

STORE_ORDER = ["Brickmania", "BRICKSHOP", "Bricking", "PlayOne"]

# ── LEGO theme categories (used by normalize_theme_fn) ────────────────────────
# These map Shopify product_type / tags → clean theme names shown in the UI

LEGO_THEMES = [
    "Architecture", "Art", "Avatar", "Batman", "BrickHeadz",
    "City", "Classic", "Creator", "DC", "Disney", "Dots",
    "DREAMZzz", "Duplo", "Education", "Exclusives", "Fast & Furious",
    "Ferrari", "Flower", "Formula 1", "Friends", "GWP",
    "Harry Potter", "Icons", "Ideas", "Indiana Jones",
    "Insiders", "Jurassic World", "Minecraft", "Minifigures",
    "Monkie Kid", "NASA", "Ninjago", "Other", "Powered Up",
    "Racers", "Speed Champions", "Star Wars", "Super Heroes",
    "Super Mario", "Technic", "The Hobbit", "Vidiyo",
    "Warhammer", "Wicked", "Alien",
]

# Hardcoded store base URLs for logo fetching (stores not in DB)
HARDCODED_STORE_URLS = {
    "Brickmania": "https://thebrickmania.com",
    "Bricking":   "https://bricking.com",
    "BRICKSHOP":  "https://brickshop.me",
    "PlayOne":    "https://playone.com.lb",
    "KLAPTAP":    "https://klaptap.com",
}
