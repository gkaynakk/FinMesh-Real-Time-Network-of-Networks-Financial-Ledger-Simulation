from enum import Enum


class QueryType(str, Enum):
    TRADE = "trade"
    RECONCILIATION_ANALYTICS = "reconciliation_analytics"
    ASSET_ANALYTICS = "asset_analytics"
    SETTLEMENT_ANALYTICS = "settlement_analytics"
    CUSTODY_ANALYTICS = "custody_analytics"
    UNKNOWN = "unknown"


def classify_question(question: str) -> QueryType:
    text = question.lower()

    # A specific trade always takes priority.
    if "trd-" in text:
        return QueryType.TRADE

    # Asset / trading analytics.
    if any(
        phrase in text
        for phrase in (
            "asset",
            "symbol",
            "notional",
            "trading volume",
            "traded quantity",
            "total quantity",
        )
    ):
        return QueryType.ASSET_ANALYTICS

    # Settlement analytics.
    if any(
        phrase in text
        for phrase in (
            "settlement",
            "settled",
            "settlement failure",
            "settlement failed",
        )
    ):
        return QueryType.SETTLEMENT_ANALYTICS

    # Custody analytics.
    if any(
        phrase in text
        for phrase in (
            "custody",
            "delivered",
            "blocked",
            "asset movement",
        )
    ):
        return QueryType.CUSTODY_ANALYTICS

    # Overall reconciliation / lifecycle analytics.
    if any(
        phrase in text
        for phrase in (
            "reconciliation",
            "consistent",
            "failed trades",
            "trade status",
            "status distribution",
        )
    ):
        return QueryType.RECONCILIATION_ANALYTICS

    return QueryType.UNKNOWN