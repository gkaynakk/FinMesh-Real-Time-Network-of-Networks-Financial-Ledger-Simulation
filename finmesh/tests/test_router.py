from intelligence.router import QueryType, classify_question


def test_trade_question():
    assert (
        classify_question("Why did TRD-ABC123 fail?")
        == QueryType.TRADE
    )


def test_settlement_question():
    assert (
        classify_question("How many trades have settlement failures?")
        == QueryType.SETTLEMENT_ANALYTICS
    )


def test_custody_question():
    assert (
        classify_question("How many custody movements are blocked?")
        == QueryType.CUSTODY_ANALYTICS
    )


def test_reconciliation_question():
    assert (
        classify_question("What is the reconciliation status distribution?")
        == QueryType.RECONCILIATION_ANALYTICS
    )
def test_asset_question():
    assert (
        classify_question("Which asset has the highest notional value?")
        == QueryType.ASSET_ANALYTICS
    )


def test_unknown_question():
    assert (
        classify_question("What is the meaning of life?")
        == QueryType.UNKNOWN
    )


def test_trade_id_takes_priority():
    assert (
        classify_question(
            "Why did settlement fail for TRD-ABC123?"
        )
        == QueryType.TRADE
    )
