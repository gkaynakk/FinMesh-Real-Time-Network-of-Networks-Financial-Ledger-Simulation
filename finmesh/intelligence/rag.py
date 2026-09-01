import json
import os

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
