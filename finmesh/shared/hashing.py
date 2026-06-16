import hashlib
import json
from typing import Any


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def calculate_event_hash(previous_hash: str | None, payload: dict[str, Any]) -> str:
    base = f"{previous_hash or ''}{canonical_json(payload)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()