from unittest.mock import MagicMock, patch
import logging

from core.ledger_verifier.main import main
from shared.hashing import calculate_event_hash


def build_valid_chain():
    payload_1 = {
        "type": "TRANSFER",
        "amount": 100,
        "currency": "USD",
    }

    hash_1 = calculate_event_hash(None, payload_1)

    payload_2 = {
        "type": "SETTLEMENT",
        "amount": 100,
        "currency": "USD",
    }

    hash_2 = calculate_event_hash(hash_1, payload_2)

    return [
        (1, "event-1", payload_1, None, hash_1),
        (2, "event-2", payload_2, hash_1, hash_2),
    ]


@patch("core.ledger_verifier.main.fetch_ledger_events")
@patch("core.ledger_verifier.main.get_connection")
def test_valid_ledger_chain(mock_get_connection, mock_fetch, caplog):
    caplog.set_level(
        logging.INFO,
        logger="core.ledger_verifier.main",
    )

    connection = MagicMock()
    mock_get_connection.return_value = connection
    mock_fetch.return_value = build_valid_chain()

    main()

    assert "CHAIN_VALID verified_events=2" in caplog.text
    connection.close.assert_called_once()


@patch("core.ledger_verifier.main.fetch_ledger_events")
@patch("core.ledger_verifier.main.get_connection")
def test_tampered_payload_detected(mock_get_connection, mock_fetch, caplog):
    connection = MagicMock()
    mock_get_connection.return_value = connection

    rows = build_valid_chain()

    ledger_id, event_id, payload, previous_hash, event_hash = rows[1]

    tampered_payload = {
        **payload,
        "amount": 999,
    }

    rows[1] = (
        ledger_id,
        event_id,
        tampered_payload,
        previous_hash,
        event_hash,
    )

    mock_fetch.return_value = rows

    main()

    assert "HASH_MISMATCH" in caplog.text
    connection.close.assert_called_once()


@patch("core.ledger_verifier.main.fetch_ledger_events")
@patch("core.ledger_verifier.main.get_connection")
def test_broken_chain_detected(mock_get_connection, mock_fetch, caplog):
    connection = MagicMock()
    mock_get_connection.return_value = connection

    rows = build_valid_chain()

    ledger_id, event_id, payload, _, event_hash = rows[1]

    rows[1] = (
        ledger_id,
        event_id,
        payload,
        "invalid-previous-hash",
        event_hash,
    )

    mock_fetch.return_value = rows

    main()

    assert "CHAIN_BROKEN" in caplog.text
    connection.close.assert_called_once()
