# FinMesh Runbook

This runbook describes how to install, start, validate, operate, demonstrate,
test, and stop FinMesh locally.

FinMesh combines containerized infrastructure with Python application services
running on the host through `uv`.

---

## 1. Prerequisites

The local environment requires:

- Git
- Docker Desktop / Docker Engine with Docker Compose
- Python 3.11 or newer
- `uv`
- an OpenAI API key for the natural-language intelligence layer

Verify the main tools:

```bash
git --version
docker --version
docker compose version
python3 --version
uv --version
```

Docker must be running before FinMesh is started.

---

## 2. Clone the Repository

```bash
git clone https://github.com/gkaynakk/FinMesh-Real-Time-Network-of-Networks-Financial-Ledger-Simulation.git

cd FinMesh-Real-Time-Network-of-Networks-Financial-Ledger-Simulation/finmesh
```

---

## 3. Install Python Dependencies

FinMesh uses `uv` for Python dependency management.

Install the locked dependencies with:

```bash
uv sync
```

This creates the project virtual environment and installs the dependencies
defined by `pyproject.toml` and `uv.lock`.

Commands in the Makefile use `uv run`, so manually activating the virtual
environment is not required.

---

## 4. Environment Configuration

Create the local environment file from the provided template:

```bash
cp .env.example .env
```

The template contains configuration for:

- Redpanda / Kafka
- PostgreSQL
- ClickHouse
- Elasticsearch
- OpenAI

Add your own OpenAI API key to `.env`:

```text
OPENAI_API_KEY=your_api_key_here
```

Never commit the real `.env` file or API credentials to Git.

The repository should contain only:

```text
.env.example
```

with placeholder or non-sensitive values.

---

## 5. Start FinMesh

Start the complete platform with:

```bash
make start
```

The startup workflow performs three major operations.

### Step 1 — Start Infrastructure

Docker Compose starts the FinMesh infrastructure, including:

- Redpanda
- Redpanda Console
- PostgreSQL
- Elasticsearch
- Kibana
- ClickHouse
- Apache Flink
- Grafana
- Apache Airflow

### Step 2 — Start Application Services

The Makefile starts the host-side Python services using `uv run`.

These include:

```text
validator
exchange
settlement
custody
clickhouse_writer
ledger_writer
elasticsearch_writer
bank
```

Process IDs are stored under:

```text
.run/
```

Application logs are written under:

```text
logs/
```

### Step 3 — Submit Flink Reconciliation

The startup process submits:

```text
flink/sql/reconciliation_to_kafka.sql
```

to the Flink cluster.

The job consumes FinMesh lifecycle topics and publishes stateful reconciliation
results to:

```text
reconciliation.results.v2
```

---

## 6. Check Platform Health

After startup, run:

```bash
make health
```

A healthy environment should report infrastructure checks similar to:

```text
FinMesh Health Check
--------------------
✓ Docker Compose reachable
✓ Redpanda healthy
✓ PostgreSQL healthy
✓ ClickHouse healthy
✓ Elasticsearch healthy
✓ Flink JobManager healthy
✓ Airflow healthy
✓ Grafana healthy
```

The command also reports the host-side application processes:

```text
✓ bank RUNNING
✓ clickhouse_writer RUNNING
✓ custody RUNNING
✓ elasticsearch_writer RUNNING
✓ exchange RUNNING
✓ ledger_writer RUNNING
✓ settlement RUNNING
✓ validator RUNNING
```

Process IDs will vary between runs.

---

## 7. Inspect Application Process State

Application processes can be checked independently with:

```bash
make pipeline-status
```

This reads the PID files under `.run/` and checks whether each process is still
running.

---

## 8. Inspect Application Logs

Follow all FinMesh application logs with:

```bash
make logs
```

Individual log files are available under:

```text
logs/
```

Examples include:

```text
logs/bank.log
logs/validator.log
logs/exchange.log
logs/settlement.log
logs/custody.log
logs/clickhouse_writer.log
logs/ledger_writer.log
logs/elasticsearch_writer.log
```

Use `Ctrl+C` to stop following the logs.

---

## 9. Inspect Redpanda Topics

List the FinMesh topics with:

```bash
docker exec finmesh-redpanda rpk topic list
```

The main topics are:

```text
raw.trade_orders
approved.trade_orders
rejected.trade_orders
exchange.trade_executions
settlement.events
custody.asset_movements
reconciliation.results.v2
```

Redpanda Console is also available at:

```text
http://localhost:8080
```

---

## 10. Run a Deterministic Demo Trade

FinMesh includes a demo producer for creating a known valid trade without
waiting for the random Bank Network generator.

Run:

```bash
uv run python -m demo.run_demo_trade
```

The command prints a generated ID similar to:

```text
TRD-DEMO-6C3E90
```

The exact ID changes on every run.

The trade is submitted to:

```text
raw.trade_orders
```

and then moves through the normal FinMesh lifecycle.

Wait briefly for the asynchronous pipeline to process the transaction.

---

## 11. Query the FinMesh Intelligence Layer

Start the interactive intelligence CLI:

```bash
uv run python -m intelligence.main
```

You should see:

```text
FinMesh Intelligence
Type 'exit' to quit.

FinMesh>
```

### Trade Lifecycle Query

Using the demo trade ID:

```text
FinMesh> What happened to TRD-DEMO-6C3E90?
```

FinMesh routes transaction-specific questions to Elasticsearch.

The retrieved lifecycle evidence is supplied to the LLM, which reconstructs the
transaction in natural language.

A response may describe:

```text
Order approval
Execution
Settlement
Custody
Reconciliation
```

including concrete failure reasons when they exist.

### Aggregate Analytics Queries

Examples include:

```text
FinMesh> How many trades failed settlement?

FinMesh> What is the reconciliation status distribution?

FinMesh> Which asset has the highest notional value?

FinMesh> How many custody movements are blocked?
```

These questions are routed to ClickHouse.

The LLM explains the query results but does not generate the underlying counts.

Type:

```text
exit
```

to leave the CLI.

---

## 12. Elasticsearch Inspection

FinMesh indexes transaction lifecycle events in:

```text
finmesh-events
```

Check Elasticsearch:

```bash
curl -s http://localhost:9200
```

Inspect the index:

```bash
curl -s "http://localhost:9200/finmesh-events/_search?pretty"
```

Kibana is available at:

```text
http://localhost:5601
```

Elasticsearch is primarily used by FinMesh for transaction-specific lifecycle
retrieval.

---

## 13. ClickHouse Inspection

Open the ClickHouse client:

```bash
docker exec -it finmesh-clickhouse clickhouse-client
```

Then select the FinMesh database:

```sql
USE finmesh;
```

Example analytics query:

```sql
SELECT
    reconciliation_status,
    count() AS trades
FROM
(
    SELECT
        trade_id,
        argMax(reconciliation_status, inserted_at) AS reconciliation_status
    FROM reconciliation_results
    GROUP BY trade_id
)
GROUP BY reconciliation_status
ORDER BY trades DESC;
```

The `argMax` operation is important because reconciliation state evolves during
the transaction lifecycle.

Counting every stored snapshot directly would overcount trades.

Exit the ClickHouse client with:

```text
exit
```

---

## 14. Verify the Hash-Chained Ledger

Run:

```bash
make verify
```

The ledger verifier checks the PostgreSQL audit chain.

For each ledger record it verifies:

```text
stored previous_hash == previous record's event_hash
```

and:

```text
recalculated event_hash == stored event_hash
```

A valid ledger should report a successful chain verification.

The exact number of verified events depends on how long FinMesh has been
running.

---

## 15. Run Automated Tests

Run the complete test suite with:

```bash
uv run pytest -v
```

The suite covers areas including:

- deterministic canonical JSON
- SHA-256 event hashing
- payload tamper detection
- broken ledger-chain detection
- reconciliation behavior
- ClickHouse analytics
- Elasticsearch lifecycle retrieval
- RAG context construction
- normal trade ID extraction
- demo trade ID extraction
- query classification and routing

A successful run should finish with all tests passing.

The exact test count may increase as the project evolves.

---

## 16. GitHub Actions CI

FinMesh uses GitHub Actions for continuous integration.

The workflow is intended to validate changes pushed to the repository and pull
requests.

CI checks include project installation/testing and repository-level validation
defined by the workflow under:

```text
.github/workflows/
```

The local pytest suite should be run before pushing changes:

```bash
uv run pytest -v
```

---

## 17. Flink Monitoring

The Flink JobManager interface is available at:

```text
http://localhost:8081
```

Use it to inspect the reconciliation streaming job and its runtime state.

The reconciliation SQL definition is stored at:

```text
flink/sql/reconciliation_to_kafka.sql
```

The job can also be submitted manually with:

```bash
make flink-reconciliation
```

---

## 18. Grafana

Grafana is available at:

```text
http://localhost:3000
```

Default local credentials are configured by Docker Compose:

```text
username: admin
password: admin
```

These credentials are intended only for local development.

Grafana is connected to the analytical/observability side of the FinMesh
environment.

---

## 19. Airflow

The Airflow web interface is available at:

```text
http://localhost:8082
```

Default local credentials are:

```text
username: admin
password: admin
```

FinMesh currently defines three operational DAGs:

| DAG | Schedule | Purpose |
|---|---|---|
| `finmesh_health_audit` | Every 15 minutes | Infrastructure health checks |
| `finmesh_reconciliation_audit` | Hourly | Reconciliation data-quality audit |
| `finmesh_ledger_verification` | Daily | Hash-chain ledger verification |

Airflow provides scheduled operational validation around the streaming platform.
It does not control the core Bank → Validator → Exchange → Settlement → Custody
transaction flow.

---

## 20. Stop FinMesh

Stop the complete environment with:

```bash
make stop
```

The shutdown workflow:

1. terminates host-side FinMesh application processes,
2. removes their PID files,
3. stops and removes the Docker Compose stack.

Docker Compose uses an extended shutdown timeout so services such as
Elasticsearch, Airflow, Flink, and Grafana have enough time to terminate
cleanly.

---

## 21. Verify Shutdown

After stopping FinMesh:

```bash
docker compose ps -a
```

A complete shutdown should return no FinMesh Compose services.

You can also check:

```bash
make pipeline-status
```

There should be no active FinMesh application processes managed by the
Makefile.

---

# Troubleshooting

## Docker Is Not Running

Check:

```bash
docker info
```

If Docker is unavailable, start Docker Desktop or the Docker daemon before
running:

```bash
make start
```

---

## A Python Service Does Not Start

Check the corresponding log:

```bash
cat logs/<service>.log
```

For example:

```bash
cat logs/elasticsearch_writer.log
```

Also verify the environment:

```bash
uv sync
```

and test imports:

```bash
uv run python -c "import intelligence; import core; import shared"
```

---

## Intelligence Reports Missing OpenAI Credentials

If the intelligence layer reports a missing API key, verify that `.env`
contains:

```text
OPENAI_API_KEY=your_api_key_here
```

Do not place the real key in `.env.example`.

Restart the intelligence CLI after changing the environment.

---

## A Trade Cannot Be Found

FinMesh processing is asynchronous.

After submitting a demo trade, wait briefly before querying it.

Make sure Elasticsearch is healthy:

```bash
curl -s http://localhost:9200
```

Then verify that lifecycle documents exist:

```bash
curl -s "http://localhost:9200/finmesh-events/_search?pretty"
```

Also confirm that the Elasticsearch Writer is running:

```bash
make pipeline-status
```

---

## Flink Reconciliation Is Missing

Check the Flink JobManager:

```text
http://localhost:8081
```

The job can be submitted manually with:

```bash
make flink-reconciliation
```

Also confirm that Redpanda is running and that the expected topics exist:

```bash
docker exec finmesh-redpanda rpk topic list
```

---

## ClickHouse Analytics Look Too Large

Do not interpret the raw number of rows in:

```text
reconciliation_results
```

as the number of unique trades.

FinMesh stores reconciliation snapshots as lifecycle state evolves.

Use the latest state per trade with:

```sql
argMax(reconciliation_status, inserted_at)
```

grouped by:

```text
trade_id
```

for final-state analytics.

---

## `make stop` Reports Container Shutdown Errors

Some infrastructure services require additional time to terminate.

The FinMesh Makefile uses an extended Docker Compose timeout for this reason.

If manual cleanup is ever required, run:

```bash
docker compose down --timeout 30
```

Then verify:

```bash
docker compose ps -a
```

---

# Recommended Validation Sequence

For a complete local validation of FinMesh:

```bash
uv sync

make start

make health

uv run python -m demo.run_demo_trade

uv run python -m intelligence.main

make verify

uv run pytest -v

make stop

docker compose ps -a
```

This validates:

```text
environment
    ↓
infrastructure
    ↓
application services
    ↓
stream processing
    ↓
end-to-end transaction lifecycle
    ↓
search and analytics
    ↓
intelligence layer
    ↓
ledger integrity
    ↓
automated tests
    ↓
clean shutdown
```

---

# Operational Interfaces

| Component | Local Interface |
|---|---|
| Redpanda Console | `http://localhost:8080` |
| Flink JobManager | `http://localhost:8081` |
| Airflow | `http://localhost:8082` |
| Grafana | `http://localhost:3000` |
| Kibana | `http://localhost:5601` |
| Elasticsearch | `http://localhost:9200` |
| ClickHouse HTTP | `http://localhost:8123` |
| PostgreSQL | `localhost:5433` |

These interfaces are configured for local development and demonstration rather
than production deployment.

---

# Summary

The normal FinMesh operating workflow is intentionally small:

```bash
make start
make health
uv run python -m demo.run_demo_trade
uv run python -m intelligence.main
make verify
uv run pytest -v
make stop
```

The surrounding infrastructure exists to demonstrate the larger architecture,
but these commands provide the main developer workflow for running and
validating the complete FinMesh system.