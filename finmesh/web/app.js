const tradeForm = document.getElementById("trade-form");
const tradeInput = document.getElementById("trade-id");
const tradeResult = document.getElementById("trade-result");


function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


function formatTimestamp(timestamp) {
    if (!timestamp) {
        return "Unknown time";
    }

    return new Date(timestamp).toLocaleString();
}


function getEventPresentation(event) {
    const topic = event.topic ?? "";
    const payload = event.payload ?? {};

    if (topic === "approved.trade_orders") {
        return {
            title: "Order Approved",
            status: payload.validation_status ?? "APPROVED",
            description:
                `${payload.side ?? ""} ${payload.quantity ?? ""} ` +
                `${payload.asset ?? ""} @ ${payload.price ?? ""} ` +
                `${payload.currency ?? ""}`,
        };
    }

    if (topic === "exchange.trade_executions") {
        return {
            title: "Trade Executed",
            status: "EXECUTED",
            description:
                `${payload.quantity ?? ""} ${payload.asset ?? ""} ` +
                `executed @ ${payload.execution_price ?? ""}`,
        };
    }

    if (topic === "settlement.events") {
        return {
            title: "Settlement",
            status: payload.status ?? "UNKNOWN",
            description: payload.reason
                ? `Reason: ${payload.reason}`
                : "Settlement event processed",
        };
    }

    if (topic === "custody.asset_movements") {
        return {
            title: "Custody",
            status: payload.status ?? "UNKNOWN",
            description: payload.reason
                ? `Reason: ${payload.reason}`
                : "Custody movement processed",
        };
    }

    if (
        topic === "reconciliation.results" ||
        topic === "reconciliation.results.v2"
    ) {
        return {
            title: "Reconciliation",
            status: payload.reconciliation_status ?? "UNKNOWN",
            description: "Cross-network lifecycle reconciliation",
        };
    }

    return {
        title: event.event_type ?? topic ?? "FinMesh Event",
        status: "RECORDED",
        description: topic,
    };
}


function getStatusClass(status) {
    const normalized = String(status).toUpperCase();

    if (
        [
            "APPROVED",
            "EXECUTED",
            "SETTLED",
            "DELIVERED",
            "CONSISTENT",
        ].includes(normalized)
    ) {
        return "status-success";
    }

    if (
        [
            "FAILED",
            "SETTLEMENT_FAILED",
            "BLOCKED",
            "CUSTODY_BLOCKED",
        ].includes(normalized)
    ) {
        return "status-danger";
    }

    return "status-pending";
}


function renderTrade(data) {
    const firstEvent = data.events[0];
    const firstPayload = firstEvent?.payload ?? {};

    const asset = firstPayload.asset ?? "Unknown asset";
    const side = firstPayload.side ?? "";
    const quantity = firstPayload.quantity ?? "";

    const eventsHtml = data.events
        .map((event) => {
            const presentation = getEventPresentation(event);
            const statusClass = getStatusClass(
                presentation.status
            );

            const rawPayload = escapeHtml(
                JSON.stringify(event.payload ?? {}, null, 2)
            );

            return `
                <div class="lifecycle-step">
                    <div class="lifecycle-rail">
                        <div class="lifecycle-marker ${statusClass}">
                            ✓
                        </div>
                        <div class="lifecycle-line"></div>
                    </div>

                    <div class="lifecycle-card">
                        <div class="lifecycle-card-header">
                            <div>
                                <div class="lifecycle-title">
                                    ${escapeHtml(presentation.title)}
                                </div>

                                <div class="lifecycle-time">
                                    ${escapeHtml(
                                        formatTimestamp(event.timestamp)
                                    )}
                                </div>
                            </div>

                            <span class="status-badge ${statusClass}">
                                ${escapeHtml(presentation.status)}
                            </span>
                        </div>

                        <div class="lifecycle-description">
                            ${escapeHtml(presentation.description)}
                        </div>

                        <details class="event-details">
                            <summary>View event details</summary>
                            <pre>${rawPayload}</pre>
                        </details>
                    </div>
                </div>
            `;
        })
        .join("");

    tradeResult.innerHTML = `
        <div class="trade-overview">
            <div>
                <p class="eyebrow">TRADE</p>
                <h4>${escapeHtml(data.trade_id)}</h4>
            </div>

            <div class="trade-metadata">
                <span>${escapeHtml(asset)}</span>
                ${
                    side
                        ? `<span>${escapeHtml(side)}</span>`
                        : ""
                }
                ${
                    quantity !== ""
                        ? `<span>Qty ${escapeHtml(quantity)}</span>`
                        : ""
                }
                <span>${data.event_count} events</span>
            </div>
        </div>

        <div class="lifecycle">
            ${eventsHtml}
        </div>
    `;
}


async function loadTrade(tradeId) {
    tradeResult.innerHTML = `
        <div class="loading">
            Loading lifecycle for ${escapeHtml(tradeId)}...
        </div>
    `;

    try {
        const response = await fetch(
            `/trades/${encodeURIComponent(tradeId)}`
        );

        if (!response.ok) {
            const error = await response.json();

            throw new Error(
                error.detail ?? "Unable to load trade lifecycle"
            );
        }

        const data = await response.json();

        renderTrade(data);
    } catch (error) {
        tradeResult.innerHTML = `
            <div class="error">
                ${escapeHtml(error.message)}
            </div>
        `;
    }
}


tradeForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const tradeId = tradeInput.value.trim();

    if (!tradeId) {
        tradeResult.innerHTML = `
            <div class="error">
                Enter a trade ID first.
            </div>
        `;
        return;
    }

    await loadTrade(tradeId);
});
async function loadAnalytics() {
    const [
        reconciliationResponse,
        settlementResponse,
        custodyResponse,
        assetResponse,
    ] = await Promise.all([
        fetch("/analytics/reconciliation"),
        fetch("/analytics/settlement"),
        fetch("/analytics/custody"),
        fetch("/analytics/assets"),
    ]);

    const responses = [
        reconciliationResponse,
        settlementResponse,
        custodyResponse,
        assetResponse,
    ];

    if (responses.some((response) => !response.ok)) {
        throw new Error("One or more analytics requests failed");
    }

    const reconciliation = await reconciliationResponse.json();
    const settlement = await settlementResponse.json();
    const custody = await custodyResponse.json();
    const assets = await assetResponse.json();

    renderSimpleMetrics(
        "reconciliation-metrics",
        reconciliation.results,
        "reconciliation_status"
    );

    renderSimpleMetrics(
        "settlement-metrics",
        settlement.results,
        "settlement_status"
    );

    renderSimpleMetrics(
        "custody-metrics",
        custody.results,
        "custody_status"
    );

    renderAssets(assets.results);
}


function renderSimpleMetrics(elementId, rows, labelKey) {
    const element = document.getElementById(elementId);

    if (!rows.length) {
        element.innerHTML = `
            <span class="metric-label">No data available</span>
        `;
        return;
    }

    element.innerHTML = rows
        .map(
            (row) => `
                <div class="metric-row">
                    <span class="metric-label">
                        ${escapeHtml(row[labelKey] ?? "UNKNOWN")}
                    </span>

                    <span class="metric-value">
                        ${Number(row.trades ?? 0).toLocaleString()}
                    </span>
                </div>
            `
        )
        .join("");
}


function renderAssets(rows) {
    const element = document.getElementById("asset-metrics");

    if (!rows.length) {
        element.innerHTML = `
            <span class="metric-label">No asset data available</span>
        `;
        return;
    }

    const maxTrades = Math.max(
        ...rows.map((row) => Number(row.trades ?? 0)),
        1
    );

    element.innerHTML = rows
        .map((row) => {
            const trades = Number(row.trades ?? 0);
            const percentage = (trades / maxTrades) * 100;

            return `
                <div class="asset-row">
                    <div class="asset-row-header">
                        <span class="asset-name">
                            ${escapeHtml(row.asset ?? "UNKNOWN")}
                        </span>

                        <span class="asset-meta">
                            ${trades.toLocaleString()} trades
                        </span>
                    </div>

                    <div class="asset-bar">
                        <div
                            class="asset-bar-fill"
                            style="width: ${percentage}%"
                        ></div>
                    </div>

                    <div class="asset-meta">
                        Qty:
                        ${Number(
                            row.total_quantity ?? 0
                        ).toLocaleString()}
                        · Notional:
                        $${Number(
                            row.notional_value ?? 0
                        ).toLocaleString()}
                    </div>
                </div>
            `;
        })
        .join("");
}


loadAnalytics().catch((error) => {
    console.error("Failed to load analytics:", error);

    [
        "reconciliation-metrics",
        "settlement-metrics",
        "custody-metrics",
        "asset-metrics",
    ].forEach((elementId) => {
        const element = document.getElementById(elementId);

        if (element) {
            element.innerHTML = `
                <span class="error">
                    Analytics unavailable
                </span>
            `;
        }
    });
});
const askForm = document.getElementById("ask-form");
const askQuestion = document.getElementById("ask-question");
const askResult = document.getElementById("ask-result");


askForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const question = askQuestion.value.trim();

    if (!question) {
        askResult.innerHTML = `
            <span class="error">
                Enter a question first.
            </span>
        `;
        return;
    }

    askResult.innerHTML = `
        <span class="loading">
            FinMesh is analyzing the platform...
        </span>
    `;

    try {
        const response = await fetch("/ask", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ question }),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(
                data.detail ?? "FinMesh intelligence request failed"
            );
        }

        askResult.innerHTML = `
            <div class="ask-answer-label">
                FINMESH
            </div>

            <div class="ask-answer">
                ${escapeHtml(data.answer)}
            </div>
        `;
    } catch (error) {
        askResult.innerHTML = `
            <span class="error">
                ${escapeHtml(error.message)}
            </span>
        `;
    }
});