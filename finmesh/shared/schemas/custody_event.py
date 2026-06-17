from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class CustodyStatus(str, Enum):
    DELIVERED = "DELIVERED"
    BLOCKED = "BLOCKED"


class CustodyEventCreated(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "custody_event_created"

    custody_event_id: str
    settlement_id: str
    trade_id: str

    source_network: str = "custody_network"

    status: CustodyStatus
    reason: str | None = None

    custody_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )