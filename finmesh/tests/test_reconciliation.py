import json
from unittest.mock import patch

from core.reconciliation_service.main import publish_result


def get_published_result(mock_producer):
    call = mock_producer.produce.call_args

    assert call.kwargs["topic"] == "reconciliation.results"

    return json.loads(call.kwargs["value"])


@patch("core.reconciliation_service.main.producer")
def test_complete_trade_is_consistent(mock_producer):
    state = {
        "approved_order": True,
        "execution": True,
        "settlement_status": "SETTLED",
        "custody_status": "DELIVERED",
    }

    publish_result("trade-001", state)

    result = get_published_result(mock_producer)

    assert result["trade_id"] == "trade-001"
    assert result["is_complete"] is True
    assert result["reconciliation_status"] == "CONSISTENT"

    mock_producer.flush.assert_called_once()


@patch("core.reconciliation_service.main.producer")
def test_failed_settlement_is_detected(mock_producer):
    state = {
        "approved_order": True,
        "execution": True,
        "settlement_status": "FAILED",
        "custody_status": "DELIVERED",
    }

    publish_result("trade-002", state)

    result = get_published_result(mock_producer)

    assert result["is_complete"] is False
    assert result["reconciliation_status"] == "SETTLEMENT_FAILED"


@patch("core.reconciliation_service.main.producer")
def test_blocked_custody_is_detected(mock_producer):
    state = {
        "approved_order": True,
        "execution": True,
        "settlement_status": "SETTLED",
        "custody_status": "BLOCKED",
    }

    publish_result("trade-003", state)

    result = get_published_result(mock_producer)

    assert result["is_complete"] is False
    assert result["reconciliation_status"] == "CUSTODY_BLOCKED"


@patch("core.reconciliation_service.main.producer")
def test_incomplete_trade_is_pending(mock_producer):
    state = {
        "approved_order": True,
        "execution": False,
        "settlement_status": None,
        "custody_status": None,
    }

    publish_result("trade-004", state)

    result = get_published_result(mock_producer)

    assert result["is_complete"] is False
    assert result["reconciliation_status"] == "PENDING"


@patch("core.reconciliation_service.main.producer")
def test_result_contains_lifecycle_state(mock_producer):
    state = {
        "approved_order": True,
        "execution": True,
        "settlement_status": "SETTLED",
        "custody_status": "DELIVERED",
    }

    publish_result("trade-005", state)

    result = get_published_result(mock_producer)

    assert result == {
        "trade_id": "trade-005",
        "has_approved_order": True,
        "has_execution": True,
        "settlement_status": "SETTLED",
        "custody_status": "DELIVERED",
        "is_complete": True,
        "reconciliation_status": "CONSISTENT",
    }
