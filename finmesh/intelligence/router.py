from enum import Enum


class QueryType(str, Enum):
    TRADE = "trade"
    RECONCILIATION_ANALYTICS = "reconciliation_analytics"
    SETTLEMENT_ANALYTICS = "settlement_analytics"
    CUSTODY_ANALYTICS = "custody_analytics"
    UNKNOWN = "unknown"


def classify_question(question: str) -> QueryType:
    text = question.lower()

    if "trd-" in text:
        return QueryType.TRADE

    if "settlement" in text:
        return QueryType.SETTLEMENT_ANALYTICS

    if "custody" in text:
        return QueryType.CUSTODY_ANALYTICS

    if any(
        word in text
        for word in (
            "reconciliation",
            "consistent",
            "failed trades",
            "trade status",
        )
    ):
        return QueryType.RECONCILIATION_ANALYTICS

    return QueryType.UNKNOWN
