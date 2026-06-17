from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field


class TradeExecutionCreated(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "trade_execution_created"

    execution_id: str
    trade_id: str

    source_network: str = "exchange_network" 

    asset: str
    quantity: int

    execution_price: float

    execution_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )