from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


def test_health():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "finmesh-api",
    }


def test_trade_lifecycle(monkeypatch):
    events = [
        {
            "trade_id": "TRD-TEST-123456",
            "topic": "approved.trade_orders",
            "timestamp": "2026-09-04T13:00:00+00:00",
            "payload": {
                "trade_id": "TRD-TEST-123456",
                "validation_status": "APPROVED",
            },
        }
    ]

    monkeypatch.setattr(
        "api.main.get_trade_lifecycle",
        lambda trade_id: events,
    )

    response = client.get("/trades/TRD-TEST-123456")

    assert response.status_code == 200

    data = response.json()

    assert data["trade_id"] == "TRD-TEST-123456"
    assert data["event_count"] == 1
    assert data["events"] == events


def test_trade_lifecycle_normalizes_trade_id(monkeypatch):
    captured = {}

    def fake_get_trade_lifecycle(trade_id):
        captured["trade_id"] = trade_id
        return [
            {
                "trade_id": trade_id,
                "topic": "approved.trade_orders",
                "payload": {},
            }
        ]

    monkeypatch.setattr(
        "api.main.get_trade_lifecycle",
        fake_get_trade_lifecycle,
    )

    response = client.get("/trades/trd-test-123456")

    assert response.status_code == 200
    assert captured["trade_id"] == "TRD-TEST-123456"


def test_trade_lifecycle_not_found(monkeypatch):
    monkeypatch.setattr(
        "api.main.get_trade_lifecycle",
        lambda trade_id: [],
    )

    response = client.get("/trades/TRD-DOESNOTEXIST")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "No FinMesh events found for TRD-DOESNOTEXIST"
    }


def test_asset_analytics(monkeypatch):
    results = [
        {
            "asset": "MSFT",
            "trades": 10,
            "total_quantity": 250,
            "notional_value": 62500.0,
        }
    ]

    monkeypatch.setattr(
        "api.main.get_asset_summary",
        lambda: results,
    )

    response = client.get("/analytics/assets")

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "results": results,
    }


def test_settlement_analytics(monkeypatch):
    results = [
        {
            "settlement_status": "FAILED",
            "trades": 5,
        }
    ]

    monkeypatch.setattr(
        "api.main.get_settlement_summary",
        lambda: results,
    )

    response = client.get("/analytics/settlement")

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "results": results,
    }


def test_custody_analytics(monkeypatch):
    results = [
        {
            "custody_status": "BLOCKED",
            "trades": 3,
        }
    ]

    monkeypatch.setattr(
        "api.main.get_custody_summary",
        lambda: results,
    )

    response = client.get("/analytics/custody")

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "results": results,
    }


def test_reconciliation_analytics(monkeypatch):
    results = [
        {
            "reconciliation_status": "CONSISTENT",
            "trades": 20,
        }
    ]

    monkeypatch.setattr(
        "api.main.get_reconciliation_summary",
        lambda: results,
    )

    response = client.get("/analytics/reconciliation")

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "results": results,
    }


def test_ask_finmesh(monkeypatch):
    monkeypatch.setattr(
        "api.main.answer_question",
        lambda question: "MSFT has the highest notional value.",
    )

    response = client.post(
        "/ask",
        json={
            "question": "Which asset has the highest notional value?"
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "question": "Which asset has the highest notional value?",
        "answer": "MSFT has the highest notional value.",
    }


def test_ask_finmesh_rejects_empty_question():
    response = client.post(
        "/ask",
        json={"question": "   "},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Question cannot be empty"
    }