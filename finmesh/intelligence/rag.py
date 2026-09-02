import json
import os
from intelligence.analytics import (
    get_asset_summary,
    get_custody_summary,
    get_reconciliation_summary,
    get_settlement_summary,
)
from intelligence.router import QueryType, classify_question
from openai import OpenAI
from dotenv import load_dotenv
from intelligence.retriever import get_trade_lifecycle
load_dotenv()

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.6-sol")


def build_context(events: list[dict]) -> str:
    blocks = []

    for event in events:
        blocks.append(
            json.dumps(
                {
                    "timestamp": event.get("timestamp"),
                    "topic": event.get("topic"),
                    "payload": event.get("payload"),
                },
                indent=2,
            )
        )

    return "\n\n".join(blocks)


def answer_trade_question(trade_id: str, question: str) -> str:
    events = get_trade_lifecycle(trade_id)

    if not events:
        return f"No FinMesh events found for {trade_id}."

    context = build_context(events)

    client = OpenAI()

    prompt = f"""
You are the FinMesh transaction analysis assistant.

Answer the user's question using ONLY the FinMesh event context below.

Rules:
- Do not invent events, statuses, reasons, or causes.
- If the context does not contain enough information, say so.
- Explain the lifecycle in chronological order when relevant.
- Mention concrete statuses and failure reasons from the events.
- Be concise and technical.

Trade ID:
{trade_id}

User question:
{question}

FinMesh event context:
{context}
"""

    response = client.responses.create(
        model=MODEL,
        input=prompt,
    )

    return response.output_text
def answer_analytics_question(
    question: str,
    query_type: QueryType,
) -> str:
    if query_type == QueryType.ASSET_ANALYTICS:
        data = get_asset_summary()

    elif query_type == QueryType.SETTLEMENT_ANALYTICS:
        data = get_settlement_summary()

    elif query_type == QueryType.CUSTODY_ANALYTICS:
        data = get_custody_summary()

    elif query_type == QueryType.RECONCILIATION_ANALYTICS:
        data = get_reconciliation_summary()

    else:
        return (
            "I can't answer that question from the currently "
            "supported FinMesh analytics."
        )

    client = OpenAI()

    prompt = f"""
You are the FinMesh financial intelligence assistant.

Answer the user's question using ONLY the analytics data below.

Rules:
- Do not invent numbers or statuses.
- Treat the supplied data as the authoritative FinMesh result.
- Clearly state relevant counts.
- Be concise and technical.
- If the data cannot answer the question, say so.

User question:
{question}

FinMesh analytics:
{json.dumps(data, indent=2)}
"""

    response = client.responses.create(
        model=MODEL,
        input=prompt,
    )

    return response.output_text


def answer_question(question: str) -> str:
    query_type = classify_question(question)

    if query_type == QueryType.TRADE:
        from intelligence.main import extract_trade_id

        trade_id = extract_trade_id(question)

        if not trade_id:
            return "I couldn't identify the trade ID."

        return answer_trade_question(
            trade_id=trade_id,
            question=question,
        )

    if query_type in {
        QueryType.ASSET_ANALYTICS,
        QueryType.RECONCILIATION_ANALYTICS,
        QueryType.SETTLEMENT_ANALYTICS,
        QueryType.CUSTODY_ANALYTICS,
    }:
        return answer_analytics_question(
            question=question,
            query_type=query_type,
        )

    return (
        "I can't route that question yet. "
        "Try asking about a specific trade, asset, settlement, "
        "custody, or reconciliation."
    )