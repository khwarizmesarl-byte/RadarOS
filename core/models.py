from dataclasses import dataclass
from typing import Optional


@dataclass
class StoreOffer:
    price:        Optional[float]
    availability: str
    link:         str
    discount_pct: Optional[int] = None
    stock_qty:    Optional[int] = None
