from intelligence.main import extract_trade_id
from intelligence.rag import build_context


def test_extract_trade_id():
    question = "Why did TRD-C3E49666 fail?"

    assert extract_trade_id(question) == "TRD-C3E49666"


def test_extract_demo_trade_id():
    question = "What happened to TRD-DEMO-6C3E90?"

    assert extract_trade_id(question) == "TRD-DEMO-6C3E90"


def test_extract_trade_id_is_case_insensitive():
    question = "Explain trd-c3e49666"

    assert extract_trade_id(question) == "TRD-C3E49666"


def test_extract_demo_trade_id_is_case_insensitive():
    question = "Explain trd-demo-abc123"

    assert extract_trade_id(question) == "TRD-DEMO-ABC123"


def test_extract_trade_id_returns_none_when_missing():
    question = "Which trades failed settlement?"

    assert extract_trade_id(question) is None


def test_build_context_contains_event_data():
    events = [
        {
            "timestamp": "2026-09-01T08:00:00+00:00",
            "topic": "settlement.events",
            "payload": {
                "trade_id": "TRD-C3E49666",
                "status": "FAILED",
                "reason": "counterparty_timeout",
            },
        }
    ]

    context = build_context(events)

    assert "TRD-C3E49666" in context
    assert "settlement.events" in context
    assert "FAILED" in context
    assert "counterparty_timeout" in context


def test_build_context_preserves_multiple_events():
    events = [
        {
            "timestamp": "2026-09-01T08:00:00+00:00",
            "topic": "approved.trade_orders",
            "payload": {
                "trade_id": "TRD-001",
                "status": "APPROVED",
            },
        },
        {
            "timestamp": "2026-09-01T08:01:00+00:00",
            "topic": "settlement.events",
            "payload": {
                "trade_id": "TRD-001",
                "status": "FAILED",
            },
        },
    ]

    context = build_context(events)

    assert "approved.trade_orders" in context
    assert "settlement.events" in context
    assert "APPROVED" in context
    assert "FAILED" in context