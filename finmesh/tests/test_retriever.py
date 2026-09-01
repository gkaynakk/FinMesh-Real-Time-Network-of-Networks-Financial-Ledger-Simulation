from unittest.mock import MagicMock, patch

from intelligence.retriever import get_trade_lifecycle


@patch("intelligence.retriever.create_client")
def test_get_trade_lifecycle(mock_create_client):
    mock_es = MagicMock()
    mock_create_client.return_value = mock_es

    mock_es.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "trade_id": "TRD-001",
                        "topic": "approved.trade_orders",
                        "timestamp": "2026-09-01T08:00:00+00:00",
                        "payload": {
                            "trade_id": "TRD-001",
                            "status": "APPROVED",
                        },
                    }
                },
                {
                    "_source": {
                        "trade_id": "TRD-001",
                        "topic": "settlement.events",
                        "timestamp": "2026-09-01T08:01:00+00:00",
                        "payload": {
                            "trade_id": "TRD-001",
                            "status": "FAILED",
                        },
                    }
                },
            ]
        }
    }

    events = get_trade_lifecycle("TRD-001")

    assert len(events) == 2
    assert events[0]["topic"] == "approved.trade_orders"
    assert events[1]["topic"] == "settlement.events"

    mock_es.search.assert_called_once_with(
        index="finmesh-events",
        size=100,
        query={
            "term": {
                "trade_id.keyword": "TRD-001"
            }
        },
        sort=[
            {
                "timestamp": {
                    "order": "asc"
                }
            }
        ],
    )

    mock_es.close.assert_called_once()
