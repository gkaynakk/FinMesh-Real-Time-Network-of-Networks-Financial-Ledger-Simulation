from shared.hashing import canonical_json, calculate_event_hash


def test_canonical_json_is_deterministic():
    payload_a = {
        "amount": 100,
        "currency": "USD",
        "account": "A123",
    }

    payload_b = {
        "account": "A123",
        "currency": "USD",
        "amount": 100,
    }

    assert canonical_json(payload_a) == canonical_json(payload_b)


def test_event_hash_is_deterministic():
    payload = {
        "amount": 250,
        "currency": "USD",
    }

    hash_a = calculate_event_hash(None, payload)
    hash_b = calculate_event_hash(None, payload)

    assert hash_a == hash_b


def test_payload_change_changes_hash():
    payload_a = {
        "amount": 100,
        "currency": "USD",
    }

    payload_b = {
        "amount": 101,
        "currency": "USD",
    }

    hash_a = calculate_event_hash(None, payload_a)
    hash_b = calculate_event_hash(None, payload_b)

    assert hash_a != hash_b


def test_previous_hash_changes_event_hash():
    payload = {
        "amount": 100,
        "currency": "USD",
    }

    hash_a = calculate_event_hash("previous-hash-A", payload)
    hash_b = calculate_event_hash("previous-hash-B", payload)

    assert hash_a != hash_b


def test_hash_is_sha256_hex_digest():
    payload = {
        "amount": 100,
        "currency": "USD",
    }

    event_hash = calculate_event_hash(None, payload)

    assert len(event_hash) == 64
    int(event_hash, 16)
