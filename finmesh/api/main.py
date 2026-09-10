from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pathlib import Path

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from intelligence.rag import answer_question

from intelligence.analytics import (
    get_asset_summary,
    get_custody_summary,
    get_reconciliation_summary,
    get_settlement_summary,
)
from intelligence.retriever import get_trade_lifecycle


app = FastAPI(
    title="FinMesh API",
    description="API for FinMesh financial network intelligence and analytics",
    version="1.1.0",
)
BASE_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = BASE_DIR / "web"

app.mount(
    "/static",
    StaticFiles(directory=WEB_DIR),
    name="static",
)
@app.get("/", include_in_schema=False)
def web_app():
    return FileResponse(WEB_DIR / "index.html")

class AskRequest(BaseModel):
    question: str

@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "healthy",
        "service": "finmesh-api",
    }


@app.get("/trades/{trade_id}")
def trade_lifecycle(trade_id: str) -> dict:
    trade_id = trade_id.upper()

    events = get_trade_lifecycle(trade_id)

    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"No FinMesh events found for {trade_id}",
        )

    return {
        "trade_id": trade_id,
        "event_count": len(events),
        "events": events,
    }


@app.get("/analytics/assets")
def asset_analytics() -> dict:
    results = get_asset_summary()

    return {
        "count": len(results),
        "results": results,
    }


@app.get("/analytics/settlement")
def settlement_analytics() -> dict:
    results = get_settlement_summary()

    return {
        "count": len(results),
        "results": results,
    }


@app.get("/analytics/custody")
def custody_analytics() -> dict:
    results = get_custody_summary()

    return {
        "count": len(results),
        "results": results,
    }


@app.get("/analytics/reconciliation")
def reconciliation_analytics() -> dict:
    results = get_reconciliation_summary()

    return {
        "count": len(results),
        "results": results,
    }

@app.post("/ask")
def ask_finmesh(request: AskRequest) -> dict[str, str]:
    question = request.question.strip()

    if not question:
        raise HTTPException(
            status_code=400,
            detail="Question cannot be empty",
        )

    answer = answer_question(question)

    return {
        "question": question,
        "answer": answer,
    }