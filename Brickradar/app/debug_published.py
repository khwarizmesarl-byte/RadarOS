import httpx
from datetime import datetime, timezone, timedelta

STORES = {
    "Brickmania": "https://brickmania.com.lb",
    "Bricking": "https://bricking.com.lb",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}

for name, base in STORES.items():
    url = f"{base}/products.json?limit=5&page=1"
    try:
        r = httpx.get(url, timeout=15, follow_redirects=True, headers=HEADERS)
        products = r.json().get("products", [])
        print(f"\n=== {name} (first 5 products) ===")
        for p in products:
            pub = p.get("published_at") or p.get("created_at") or "MISSING"
            title = p.get("title", "")[:50]
            is_new = False
            if pub and pub != "MISSING":
                try:
                    dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    days_ago = (datetime.now(timezone.utc) - dt).days
                    is_new = days_ago <= 30
                    print(f"  {title} | published={pub[:10]} | {days_ago}d ago | is_new={is_new}")
                except Exception as e:
                    print(f"  {title} | parse error: {e}")
            else:
                print(f"  {title} | published_at=MISSING")
    except Exception as e:
        print(f"{name} error: {e}")
