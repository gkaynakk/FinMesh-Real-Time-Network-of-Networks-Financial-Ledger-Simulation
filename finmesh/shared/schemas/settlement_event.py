from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class SettlementStatus(str, Enum):
    SETTLED = "SETTLED"
    FAILED = "FAILED"


class SettlementEventCreated(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "settlement_event_created"

    settlement_id: str
    execution_id: str
    trade_id: str

    source_network: str = "settlement_network"

    status: SettlementStatus
    reason: str | None = None

    settlement_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )