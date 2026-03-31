"""
core/ai.py — AI assistant, store discovery, store analysis.
All FastAPI route handlers live in main.py; this module contains the logic.
"""

import json
import re
import urllib.parse
from typing import AsyncGenerator, Optional

import httpx
from bs4 import BeautifulSoup
from fastapi.responses import StreamingResponse, JSONResponse

from core.db import db_connect, meta_get


# ── Context builder ────────────────────────────────────────────────────────────

def build_context(db_path: str, page: str = "dashboard", module_name: str = "BrickRadar") -> str:
    """Build a compact context snapshot for the AI assistant."""
    conn = db_connect(db_path)
    cur  = conn.cursor()

    lsf = """
        AND captured_at IN (
            SELECT MAX(captured_at) FROM snapshots GROUP BY store
        )
    """

    # RadarList with current prices
    cur.execute("SELECT item_number, title, added_price, added_store FROM radarlist ORDER BY added_at DESC")
    radar       = [dict(r) for r in cur.fetchall()]
    radar_lines = []
    for item in radar[:10]:
        cur.execute(
            f"SELECT store, price FROM snapshots WHERE item_number=? {lsf} ORDER BY price ASC",
            (item["item_number"],)
        )
        prices    = [f"{r['store']}:${r['price']}" for r in cur.fetchall() if r["price"]]
        min_price = min((float(p.split("$")[1]) for p in prices), default=None)
        drop      = round(item["added_price"] - min_price, 2) if item["added_price"] and min_price else 0
        radar_lines.append(
            f"- #{item['item_number']} {item['title']} | added@${item['added_price']} | now: {', '.join(prices)} | drop:${drop}"
        )

    # Top deals
    cur.execute(f"""
        SELECT item_number, title, store, price, compare_at,
               ROUND((compare_at-price)/compare_at*100,1) as pct
        FROM snapshots WHERE compare_at>price AND compare_at>0 {lsf}
        ORDER BY pct DESC LIMIT 8
    """)
    deal_lines = [
        f"- #{r['item_number']} {r['title']} | {r['store']} ${r['price']} (was ${r['compare_at']}, -{r['pct']}%)"
        for r in cur.fetchall()
    ]

    # Store summary
    cur.execute(f"""
        SELECT store, COUNT(*) as cnt, ROUND(AVG(price),2) as avg_p
        FROM snapshots WHERE price>0 {lsf}
        GROUP BY store ORDER BY avg_p ASC
    """)
    store_lines = [f"- {r['store']}: {r['cnt']} items, avg ${r['avg_p']}" for r in cur.fetchall()]

    last_updated = meta_get(db_path, "last_updated") or "unknown"

    if page == "analytics":
        cur.execute(f"""
            SELECT tags, COUNT(*) as cnt FROM snapshots
            WHERE tags IS NOT NULL AND tags != '' {lsf}
            GROUP BY tags ORDER BY cnt DESC LIMIT 15
        """)
        theme_lines = [f"- {r['tags']}: {r['cnt']} products" for r in cur.fetchall()]
        conn.close()
        return f"""You are {module_name} AI, a LEGO price assistant for stores in Lebanon.
Last refresh: {last_updated}
PAGE: Analytics — market overview. Focus on trends, rankings, store comparisons.
STORES: {chr(10).join(store_lines)}
TOP DEALS: {chr(10).join(deal_lines) if deal_lines else "None"}
THEME BREAKDOWN: {chr(10).join(theme_lines) if theme_lines else "No data"}
Be concise. Format prices as $XX.XX."""

    elif page == "advanced":
        cur.execute(f"""
            SELECT item_number, title, COUNT(DISTINCT store) as sc,
                   MIN(price) as mn, MAX(price) as mx,
                   ROUND(MAX(price)-MIN(price),2) as spread
            FROM snapshots WHERE price>0 {lsf}
            GROUP BY item_number HAVING sc >= 2 ORDER BY spread DESC LIMIT 15
        """)
        spread_lines = [
            f"- #{r['item_number']} {r['title']} | {r['sc']} stores | ${r['mn']}-${r['mx']} spread:${r['spread']}"
            for r in cur.fetchall()
        ]
        conn.close()
        return f"""You are {module_name} AI, a LEGO price assistant for stores in Lebanon.
Last refresh: {last_updated}
PAGE: Advanced Analysis — cross-store price comparisons. Focus on price spreads and deals.
STORES: {chr(10).join(store_lines)}
PRICE SPREAD (2+ stores): {chr(10).join(spread_lines) if spread_lines else "No data"}
Be concise. Format prices as $XX.XX."""

    else:
        conn.close()
        return f"""You are {module_name} AI, a LEGO price assistant for stores in Lebanon.
Last refresh: {last_updated}
PAGE: {page}
STORES (cheapest to priciest avg):
{chr(10).join(store_lines)}
TOP DEALS:
{chr(10).join(deal_lines) if deal_lines else "None"}
RADARLIST:
{chr(10).join(radar_lines) if radar_lines else "Empty"}
Be concise. Format prices as $XX.XX. If data not in context, say so."""


# ── Streaming chat ─────────────────────────────────────────────────────────────

async def stream_chat(
    db_path: str,
    messages: list,
    page: str,
    anthropic_api_key: str,
    groq_api_key: str,
    module_name: str = "BrickRadar",
) -> AsyncGenerator[str, None]:
    """Yield SSE chunks for the AI chat stream."""
    use_groq = not anthropic_api_key and bool(groq_api_key)
    context  = build_context(db_path, page, module_name)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            if use_groq:
                groq_messages = [{"role": "system", "content": context}] + messages
                async with client.stream(
                    "POST",
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {groq_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model":      "llama-3.1-8b-instant",
                        "messages":   groq_messages,
                        "max_tokens": 1024,
                        "stream":     True,
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        err  = body.decode()[:300]
                        print(f"[AI] Groq error {resp.status_code}: {err}")
                        yield f"data: {json.dumps({'text': f'⚠ Groq API error {resp.status_code}: {err}'})}\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            if data == "[DONE]":
                                break
                            try:
                                evt  = json.loads(data)
                                text = evt.get("choices", [{}])[0].get("delta", {}).get("content", "")
                                if text:
                                    yield f"data: {json.dumps({'text': text})}\n\n"
                            except Exception:
                                pass

            else:
                async with client.stream(
                    "POST",
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key":         anthropic_api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type":      "application/json",
                    },
                    json={
                        "model":      "claude-haiku-4-5-20251001",
                        "max_tokens": 1024,
                        "system":     context,
                        "messages":   messages,
                        "stream":     True,
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body     = await resp.aread()
                        err_text = body.decode()[:300]
                        print(f"[AI] Anthropic error {resp.status_code}: {err_text}")
                        yield f"data: {json.dumps({'text': f'⚠ API error {resp.status_code}: {err_text}'})}\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            if data == "[DONE]":
                                break
                            try:
                                evt = json.loads(data)
                                if evt.get("type") == "content_block_delta":
                                    text = evt.get("delta", {}).get("text", "")
                                    if text:
                                        yield f"data: {json.dumps({'text': text})}\n\n"
                            except Exception:
                                pass

    except Exception as e:
        print(f"[AI] stream error: {e}")
        import traceback; traceback.print_exc()
        yield f"data: {json.dumps({'text': f'⚠ Error: {str(e)}'})}\n\n"

    yield "data: [DONE]\n\n"


# ── Store analysis ─────────────────────────────────────────────────────────────

async def analyze_store(
    url: str,
    platform: str,
    product_count: int,
    samples: list,
    anthropic_api_key: str,
    groq_api_key: str,
) -> dict:
    """Use AI to suggest scraper config for a newly discovered store."""
    prompt = f"""You are analyzing a LEGO/toy store to suggest the best scraper configuration for BrickRadar.

Store URL: {url}
Detected platform: {platform}
Products found: {product_count}
Sample products: {json.dumps(samples[:5], indent=2)}

Based on this data, suggest the optimal configuration. Respond ONLY with a valid JSON object, no explanation, no markdown:
{{
  "store_name": "suggested store name (short, clean, e.g. KLAPTAP)",
  "collection_slug": "slug if store has mixed products and needs filtering (e.g. lego), or null",
  "new_arrivals_slug": "slug for new arrivals collection if detectable from samples, or null",
  "vat_multiplier": 1.0,
  "lego_only": false,
  "warnings": ["any issues or notes about this store"],
  "confidence": "high/medium/low"
}}

Rules:
- store_name: extract from domain, capitalize properly
- collection_slug: only suggest if store clearly has non-LEGO products mixed in
- lego_only: true only if store has many non-LEGO brands mixed in the samples
- vat_multiplier: 1.0 unless you detect ex-VAT pricing patterns
- warnings: note if site has Cloudflare, limited products, or unusual structure"""

    use_groq = not anthropic_api_key and bool(groq_api_key)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            if use_groq:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"},
                    json={"model": "llama-3.1-8b-instant",
                          "messages": [{"role": "user", "content": prompt}], "max_tokens": 512},
                )
                text = resp.json()["choices"][0]["message"]["content"].strip()
            else:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": anthropic_api_key, "anthropic-version": "2023-06-01",
                             "content-type": "application/json"},
                    json={"model": "claude-haiku-4-5-20251001", "max_tokens": 512,
                          "messages": [{"role": "user", "content": prompt}]},
                )
                text = resp.json()["content"][0]["text"].strip()

        json_match = re.search(r'\{[\s\S]*\}', text)
        if not json_match:
            return {"ok": False, "error": "AI returned unexpected format"}
        return {"ok": True, "suggestion": json.loads(json_match.group())}

    except Exception as e:
        print(f"[AI analyze_store] error: {e}")
        return {"ok": False, "error": str(e)}


# ── Store discovery ────────────────────────────────────────────────────────────

async def stream_discover_stores(
    region: str,
    hardcoded_store_domains: list,
    anthropic_api_key: str,
    groq_api_key: str,
    tier: str = "local",
    country: str = "",
) -> AsyncGenerator[str, None]:
    """Yield SSE chunks: DuckDuckGo search + AI filtering for new stores."""
    use_groq = not anthropic_api_key and bool(groq_api_key)

    # Build country-aware search location string
    country_names = {
        "AE": "UAE Dubai", "SA": "Saudi Arabia", "KW": "Kuwait",
        "TR": "Turkey", "DE": "Germany", "GB": "UK", "CN": "China",
        "LB": "Lebanon", "US": "US global",
    }
    search_location = country_names.get(country, region) if country else region

    try:
        yield f"data: {json.dumps({'text': '', 'status': 'searching'})}\n\n"

        # Build tier-aware queries
        if tier == "official":
            queries = [
                f"LEGO compatible official brand store online shop",
                f"CaDA Mould King Cada official website store",
                f"brick building sets official brand online store",
            ]
        elif tier == "international":
            queries = [
                f"LEGO store online shop {search_location} buy sets",
                f"toy store {search_location} LEGO retailer website",
                f"LEGO sets buy online {search_location} delivery",
            ]
        else:  # local
            queries = [
                f"LEGO store online shop {search_location} buy sets",
                f"toy store {search_location} LEGO official retailer website",
                f"buy LEGO {search_location} online shop delivery",
            ]
        all_results = []

        async with httpx.AsyncClient(
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            for query in queries:
                try:
                    r    = await client.get(
                        "https://html.duckduckgo.com/html/",
                        params={"q": query, "kl": "wt-wt"},
                        follow_redirects=True,
                    )
                    soup = BeautifulSoup(r.text, "lxml")
                    for result in soup.select(".result")[:6]:
                        title_el   = result.select_one(".result__title")
                        url_el     = result.select_one(".result__url")
                        snippet_el = result.select_one(".result__snippet")
                        if title_el and url_el:
                            url = url_el.get_text(strip=True).strip()
                            if not url.startswith("http"):
                                url = "https://" + url
                            if not any(h in url for h in hardcoded_store_domains):
                                all_results.append({
                                    "title":   title_el.get_text(strip=True),
                                    "url":     url,
                                    "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                                })
                except Exception as e:
                    print(f"[DDG] error: {e}")

        # Deduplicate by domain
        seen           = set()
        unique_results = []
        for r in all_results:
            try:
                domain = urllib.parse.urlparse(r["url"]).netloc.replace("www.", "")
                if domain and domain not in seen:
                    seen.add(domain)
                    unique_results.append(r)
            except Exception:
                pass

        yield f"data: {json.dumps({'text': '', 'status': f'found {len(unique_results)} results, analyzing...'})}\n\n"

        if not unique_results:
            yield f"data: {json.dumps({'text': '[]', 'status': 'done'})}\n\n"
            yield "data: [DONE]\n\n"
            return

        # Build tier-aware AI prompt
        tier_instruction = {
            "official": "official brand manufacturer stores (like CaDA, Mould King, Reobrix, Cobi) that sell their own building block products directly",
            "international": f"online retailers that are PHYSICALLY BASED IN or PRIMARILY SERVE {search_location} — they must have a local domain (.ae, .sa, .kw etc) OR explicitly mention {search_location} in their URL/description",
            "local": f"local toy stores physically located in {search_location} that sell LEGO products online",
        }.get(tier, f"LEGO stores in {search_location}")

        geo_filter = ""
        if tier == "international" and search_location:
            geo_filter = f"""
IMPORTANT GEOGRAPHIC FILTER:
- ONLY include stores that are based in or primarily serve {search_location}
- REJECT any US, UK, or global stores (Target, Best Buy, Amazon, ToysRUs, BrickLink etc)
- REJECT stores unless their URL contains a local country indicator or they explicitly serve {search_location}
- If unsure, EXCLUDE the store
"""

        prompt = f"""You are analyzing web search results to find legitimate stores.

Here are the search results:
{json.dumps(unique_results, indent=2)}

From these results, identify {tier_instruction}.
{geo_filter}
Criteria:
- Actually sell products online (not just mention them)
- Have a real e-commerce website
- Are NOT social media pages, news articles, Wikipedia, or directories
- Are NOT already tracked: {', '.join(hardcoded_store_domains)}

Return ONLY a JSON array, no other text:
[
  {{
    "name": "Store Name",
    "url": "https://exact-url.com",
    "platform_guess": "shopify/woocommerce/bigcommerce/unknown",
    "notes": "one line about the store"
  }}
]

If none qualify, return an empty array: []"""

        full_text = ""
        async with httpx.AsyncClient(timeout=60) as client:
            if use_groq:
                async with client.stream(
                    "POST",
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"},
                    json={"model": "llama-3.1-8b-instant",
                          "messages": [{"role": "user", "content": prompt}],
                          "max_tokens": 1024, "stream": True},
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            if data == "[DONE]": break
                            try:
                                evt  = json.loads(data)
                                text = evt.get("choices", [{}])[0].get("delta", {}).get("content", "")
                                if text: full_text += text
                            except Exception: pass
            else:
                async with client.stream(
                    "POST",
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": anthropic_api_key, "anthropic-version": "2023-06-01",
                             "content-type": "application/json"},
                    json={"model": "claude-haiku-4-5-20251001", "max_tokens": 1024,
                          "messages": [{"role": "user", "content": prompt}], "stream": True},
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            if data == "[DONE]": break
                            try:
                                evt = json.loads(data)
                                if evt.get("type") == "content_block_delta":
                                    text = evt.get("delta", {}).get("text", "")
                                    if text: full_text += text
                            except Exception: pass

        yield f"data: {json.dumps({'text': full_text, 'status': 'done'})}\n\n"

    except Exception as e:
        print(f"[Discover] error: {e}")
        import traceback; traceback.print_exc()
        yield f"data: {json.dumps({'text': '[]', 'status': 'error', 'error': str(e)})}\n\n"

    yield "data: [DONE]\n\n"
