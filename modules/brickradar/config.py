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
    "enable_global_comparison": True,    # global + regional international sources defined
}

# ── API Keys ──────────────────────────────────────────────────────────────────
# Store third-party API keys here. Keep this file out of version control
# or move keys to a .env file for production.

API_KEYS = {
    "brickset": "3-2E4Q-bBzN-Srf2Z",   # Get free key at: https://brickset.com/tools/webservices/requestkey
    "ebay":      "Khwarizm-bricksRa-SBX-bf61c9ff1-6b38f348"
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
        "source_type":    "local",
        "country_code":   "LB",
    },
    "Bricking": {
        "url":            "https://bricking.com",
        "vat_multiplier": BRICKING_VAT_MULTIPLIER,
        "lego_only":      False,
        "collection_slug": "",
        "new_arrivals_collection": NEW_ARRIVAL_COLLECTIONS.get("Bricking", ""),
        "source_type":    "local",
        "country_code":   "LB",
    },
    "KLAPTAP": {
        "url":            "https://klaptap.com",
        "vat_multiplier": 1.00,
        "lego_only":      True,
        "collection_slug": "",
        "new_arrivals_collection": "",
        "source_type":    "local",
        "country_code":   "LB",
    },
}

# ── BigCommerce stores ─────────────────────────────────────────────────────────

BIGCOMMERCE_STORES = {
    "Ayoub Computers": {
        "url":            "https://ayoubcomputers.com",
        "collection_slug": "lego",
        "lego_only":      True,
        "vat_multiplier": 1.0,
        "source_type":    "local",
        "country_code":   "LB",
    },
}

# ── HTML stores (custom scrapers) ──────────────────────────────────────────────

# PlayOne: Lebanese store, uses Cloudflare — scraped via cloudscraper
PLAYONE_LISTING_URLS = [
    "https://playone.com.lb/brands/lego/",
]

# BRICKSHOP: WooCommerce store
BRICKSHOP_LISTING_URL = "https://brickshop.me/shop/"

# ── Official brand stores ──────────────────────────────────────────────────────

OFFICIAL_STORES = {
    "LEGO Official": {
        "url":          "https://www.lego.com",
        "source_type":  "official",
        "country_code": "US",
        "currency":     "USD",
        "platform":     "lego_com",
    },
    "CaDA Official": {
        "url":          "https://decadastore.com",
        "source_type":  "official",
        "country_code": "CN",
        "currency":     "USD",
        "platform":     "shopify",
        "lego_only":    False,
        "vat_multiplier": 1.0,
        "collection_slug": "",
        "new_arrivals_collection": "",
    },
    "Mould King": {
        "url":          "https://mouldking.store",
        "source_type":  "official",
        "country_code": "CN",
        "currency":     "USD",
        "platform":     "woocommerce",
        "lego_only":    False,
        "vat_multiplier": 1.0,
    },
}

# ── International retail stores ────────────────────────────────────────────────
# Two sub-levels: global platforms + regional country retailers/wholesalers
# source_type = "international", sub_tier = "global" | "regional"

INTERNATIONAL_STORES = {

    # ── Global platforms ──────────────────────────────────────────────────────
    "Bricklink": {
        "url":          "https://www.bricklink.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "US",
        "currency":     "USD",
        "platform":     "bricklink",
    },
    "Amazon US": {
        "url":          "https://www.amazon.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "US",
        "currency":     "USD",
        "platform":     "amazon",
    },
    "AliExpress": {
        "url":          "https://www.aliexpress.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "CN",
        "currency":     "USD",
        "platform":     "aliexpress",
    },
    "eBay": {
        "url":          "https://www.ebay.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "US",
        "currency":     "USD",
        "platform":     "ebay",
    },

    # ── Regional — Gulf ───────────────────────────────────────────────────────
    "Noon UAE": {
        "url":          "https://www.noon.com",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "AE",
        "currency":     "AED",
        "platform":     "noon",
    },
    "Hamleys UAE": {
        "url":          "https://www.hamleys.ae",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "AE",
        "currency":     "AED",
        "platform":     "shopify",
    },
    "Mumzworld": {
        "url":          "https://www.mumzworld.com",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "AE",
        "currency":     "AED",
        "platform":     "magento",
    },
    "Toys R Us UAE": {
        "url":          "https://www.toysrus.ae",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "AE",
        "currency":     "AED",
        "platform":     "shopify",
    },

    # ── Regional — Europe ─────────────────────────────────────────────────────
    "Alternate DE": {
        "url":          "https://www.alternate.de",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "DE",
        "currency":     "EUR",
        "platform":     "html",
    },
    "Smyths UK": {
        "url":          "https://www.smythstoys.com/uk/en-gb",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "GB",
        "currency":     "GBP",
        "platform":     "html",
    },

    # ── Regional — Turkey ─────────────────────────────────────────────────────
    "Toyzz Shop TR": {
        "url":          "https://www.toyzzshop.com",
        "source_type":  "international",
        "sub_tier":     "regional",
        "country_code": "TR",
        "currency":     "TRY",
        "platform":     "html",
    },

    # ── Regional — China ──────────────────────────────────────────────────────
    "Taobao": {
        "url":          "https://www.taobao.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "CN",
        "currency":     "CNY",
        "platform":     "taobao",
    },
    "1688": {
        "url":          "https://www.1688.com",
        "source_type":  "international",
        "sub_tier":     "global",
        "country_code": "CN",
        "currency":     "CNY",
        "platform":     "1688",
    },
}

# Country metadata for display
INTERNATIONAL_COUNTRIES = {
    "AE": {"name": "UAE",     "flag": "🇦🇪", "region": "Gulf"},
    "SA": {"name": "KSA",     "flag": "🇸🇦", "region": "Gulf"},
    "KW": {"name": "Kuwait",  "flag": "🇰🇼", "region": "Gulf"},
    "TR": {"name": "Turkey",  "flag": "🇹🇷", "region": "Europe"},
    "DE": {"name": "Germany", "flag": "🇩🇪", "region": "Europe"},
    "GB": {"name": "UK",      "flag": "🇬🇧", "region": "Europe"},
    "CN": {"name": "China",   "flag": "🇨🇳", "region": "Asia"},
    "US": {"name": "Global",  "flag": "🌍",  "region": "Global"},
    "NL": {"name": "Netherlands", "flag": "🇳🇱", "region": "Europe"},
}

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
