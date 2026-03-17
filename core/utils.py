import re
from typing import Any, Optional, Tuple


KNOWN_BRANDS = [
    "LEGO", "Mould King", "Blokees", "Nifeliz", "CaDA", "Reobrix",
    "Lumibricks", "JAKI", "Jaki", "LOZ", "GULY", "Guly", "Brickmania",
]

_ROUND_NUMBERS = {
    "1000","2000","3000","4000","5000","1500","2500",
    "1100","1200","1300","1400","1600","1700","1800","1900"
}
_QTY_RE = re.compile(r"^(piece|pieces|pcs|pc|parts|bricks|blocks|elements|in1)")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        return float(s) if s else None
    except Exception:
        return None


def utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def compute_discount_pct(price: Optional[float], compare_at: Optional[float]) -> Optional[int]:
    if price is None or compare_at is None or compare_at <= 0 or price >= compare_at:
        return None
    pct = int(round((1.0 - (price / compare_at)) * 100))
    return pct if pct > 0 else None


def extract_item_number(text: str) -> Optional[str]:
    if not text:
        return None
    t = str(text)

    # Explicit Item # tag
    m = re.search(r"Item\s*#\s*([0-9]{4,7})", t, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # SKU like "10345-LEGO"
    m = re.search(r"\b([0-9]{4,7})-[A-Za-z]+\b", t)
    if m and m.group(1) not in _ROUND_NUMBERS:
        return m.group(1)

    candidates = []
    for m in re.finditer(r"\b([0-9]{4,7})\b", t):
        num = m.group(1)
        after = t[m.end():m.end()+10].strip().lower()
        if _QTY_RE.match(after):
            continue
        candidates.append((m.start(), num))

    if not candidates:
        return None

    non_round = [num for _, num in candidates if num not in _ROUND_NUMBERS]
    if non_round:
        return non_round[-1]
    return candidates[-1][1]


def normalize_brand_from_vendor_title(vendor: str, title: str) -> str:
    v = (vendor or "").strip()
    if re.sub(r"[®\s/]", "", v).upper() == "LEGO":
        return "LEGO"
    v_first = v.split("/")[0].strip()
    if re.sub(r"[®\s]", "", v_first).upper() == "LEGO":
        return "LEGO"

    t = (title or "").strip()

    for brand in KNOWN_BRANDS:
        if t.lower().startswith(brand.lower()):
            return brand.upper()

    m = re.match(r'^([A-Z][A-Za-z0-9&-]+)', t)
    if m:
        candidate = m.group(1)
        if not candidate.isdigit() and candidate.upper() not in ("THE", "A", "AN"):
            return candidate.upper()

    if v:
        return v.split("/")[0].strip().upper()

    return "UNKNOWN"


def order_stores(stores: list) -> list:
    return sorted([s for s in stores if s], key=lambda s: s.lower())
