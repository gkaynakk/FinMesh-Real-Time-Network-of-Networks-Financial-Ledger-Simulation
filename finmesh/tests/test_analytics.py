from unittest.mock import MagicMock, patch

from intelligence.analytics import (
    get_custody_summary,
    get_reconciliation_summary,
    get_settlement_summary,
)


@patch("intelligence.analytics.get_clickhouse_client")
def test_reconciliation_summary(mock_get_client):
    client = MagicMock()
    mock_get_client.return_value = client

    result = MagicMock()
    result.result_rows = [
        ("CONSISTENT", 100),
        ("SETTLEMENT_FAILED", 20),
        ("CUSTODY_BLOCKED", 10),
    ]
    client.query.return_value = result

    summary = get_reconciliation_summary()

    assert summary == [
        {"reconciliation_status": "CONSISTENT", "trades": 100},
        {"reconciliation_status": "SETTLEMENT_FAILED", "trades": 20},
        {"reconciliation_status": "CUSTODY_BLOCKED", "trades": 10},
    ]

    client.close.assert_called_once()


@patch("intelligence.analytics.get_clickhouse_client")
def test_settlement_summary(mock_get_client):
    client = MagicMock()
    mock_get_client.return_value = client

    result = MagicMock()
    result.result_rows = [
        ("SETTLED", 100),
        ("FAILED", 20),
    ]
    client.query.return_value = result

    summary = get_settlement_summary()

    assert summary == [
        {"settlement_status": "SETTLED", "trades": 100},
        {"settlement_status": "FAILED", "trades": 20},
    ]

    client.close.assert_called_once()


@patch("intelligence.analytics.get_clickhouse_client")
def test_custody_summary(mock_get_client):
    client = MagicMock()
    mock_get_client.return_value = client

    result = MagicMock()
    result.result_rows = [
        ("DELIVERED", 100),
        ("BLOCKED", 10),
    ]
    client.query.return_value = result

    summary = get_custody_summary()

    assert summary == [
        {"custody_status": "DELIVERED", "trades": 100},
        {"custody_status": "BLOCKED", "trades": 10},
    ]

    client.close.assert_called_once()
