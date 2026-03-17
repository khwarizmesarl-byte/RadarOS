# LEGO Tracker — Project State (Checkpoint)

**Version:** v1.0-db  
**Status:** Proxy stable (external), collectors refactored to stable endpoints where possible.

## Sources
- Brickmania (Shopify): collection pages → product handle → `/products/<handle>.js` JSON
- BRICKSHOP (WooCommerce): HTML fallback (no API keys yet)

## Persistence
- SQLite file: `lego_tracker.db` (env: `LEGO_TRACKER_DB`)

## What it does
- Normalizes offers across stores
- Stores latest offers + full price history
- Generates alerts:
  - `price_drop`
  - `new_discount`
  - `back_in_stock`

## Next improvements (when keys arrive)
- Replace BRICKSHOP HTML fallback with WooCommerce REST `/wp-json/wc/v3/products`
- Add pagination crawling for BRICKSHOP categories
- Add Telegram/email notifications for alerts
