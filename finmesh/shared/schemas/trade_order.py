from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeOrderCreated(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "trade_order_created"
    trade_id: str
    source_network: str = "bank_network"
    customer_id: str
    asset: str
    side: TradeSide
    quantity: int
    price: float
    currency: str = "USD"
    event_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )