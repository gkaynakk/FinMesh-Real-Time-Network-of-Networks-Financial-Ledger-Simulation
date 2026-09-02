DOCKER_STOP_TIMEOUT ?= 30

.PHONY: up down bank validator exchange settlement custody reconciliation clickhouse ledger verify elasticsearch \
	pipeline pipeline-stop pipeline-status logs flink-reconciliation start stop health

up:
	docker compose up -d

down:
	docker compose down --timeout $(DOCKER_STOP_TIMEOUT)

bank:
	uv run python -m networks.bank_producer.main

validator:
	uv run python -m core.validator_service.main

exchange:
	uv run python -m networks.exchange_network.main

settlement:
	uv run python -m networks.settlement_network.main

custody:
	uv run python -m networks.custody_network.main

reconciliation:
	uv run python -m core.reconciliation_service.main

clickhouse:
	uv run python -m core.clickhouse_writer.main

ledger:
	uv run python -m core.ledger_writer.main

verify:
	uv run python -m core.ledger_verifier.main

elasticsearch:
	uv run python -m core.elasticsearch_writer.main


# --------------------------------------------------
# FinMesh application pipeline
# --------------------------------------------------

pipeline:
	@echo "Starting FinMesh application pipeline..."
	@mkdir -p .run logs
	@uv run python -m core.validator_service.main > logs/validator.log 2>&1 & echo $$! > .run/validator.pid
	@uv run python -m networks.exchange_network.main > logs/exchange.log 2>&1 & echo $$! > .run/exchange.pid
	@uv run python -m networks.settlement_network.main > logs/settlement.log 2>&1 & echo $$! > .run/settlement.pid
	@uv run python -m networks.custody_network.main > logs/custody.log 2>&1 & echo $$! > .run/custody.pid
	@uv run python -m core.clickhouse_writer.main > logs/clickhouse_writer.log 2>&1 & echo $$! > .run/clickhouse_writer.pid
	@uv run python -m core.ledger_writer.main > logs/ledger_writer.log 2>&1 & echo $$! > .run/ledger_writer.pid
	@uv run python -m core.elasticsearch_writer.main > logs/elasticsearch_writer.log 2>&1 & echo $$! > .run/elasticsearch_writer.pid
	@echo "Core consumers started. Waiting before producer..."
	@sleep 2
	@uv run python -m networks.bank_producer.main > logs/bank.log 2>&1 & echo $$! > .run/bank.pid
	@echo "FinMesh application pipeline started."


pipeline-stop:
	@echo "Stopping FinMesh application pipeline..."
	@for pidfile in .run/*.pid; do \
		if [ -f "$$pidfile" ]; then \
			kill $$(cat "$$pidfile") 2>/dev/null || true; \
		fi; \
	done
	@rm -f .run/*.pid
	@echo "FinMesh pipeline stopped."


pipeline-status:
	@echo "FinMesh application processes:"
	@for pidfile in .run/*.pid; do \
		if [ -f "$$pidfile" ]; then \
			name=$$(basename "$$pidfile" .pid); \
			pid=$$(cat "$$pidfile"); \
			if kill -0 $$pid 2>/dev/null; then \
				echo "  ✓ $$name (PID $$pid) RUNNING"; \
			else \
				echo "  ✗ $$name (PID $$pid) STOPPED"; \
			fi; \
		fi; \
	done


logs:
	@tail -f logs/*.log


# --------------------------------------------------
# Flink reconciliation
# --------------------------------------------------

flink-reconciliation:
	@echo "Submitting Flink reconciliation job..."
	@docker compose --profile tools run --rm flink-sql-client \
		bin/sql-client.sh -f /opt/flink/sql/reconciliation_to_kafka.sql


# --------------------------------------------------
# Full FinMesh startup
# --------------------------------------------------

start:
	@echo "Starting FinMesh infrastructure..."
	@$(MAKE) up
	@echo "Waiting for infrastructure..."
	@sleep 5
	@$(MAKE) pipeline
	@echo "Submitting Flink reconciliation..."
	@$(MAKE) flink-reconciliation
	@echo "FinMesh started."


# --------------------------------------------------
# Full FinMesh shutdown
# --------------------------------------------------

stop:
	@echo "Stopping FinMesh..."
	@$(MAKE) pipeline-stop
	@echo "Stopping Docker infrastructure..."
	@$(MAKE) down
	@echo "FinMesh stopped."


# --------------------------------------------------
# Health checks
# --------------------------------------------------

health:
	@echo ""
	@echo "FinMesh Health Check"
	@echo "--------------------"
	@docker compose ps --status running >/dev/null 2>&1 && \
		echo "✓ Docker Compose reachable" || echo "✗ Docker Compose unavailable"
	@docker exec finmesh-redpanda rpk cluster health >/dev/null 2>&1 && \
		echo "✓ Redpanda healthy" || echo "✗ Redpanda unhealthy"
	@docker exec finmesh-postgres pg_isready -U finmesh -d finmesh >/dev/null 2>&1 && \
		echo "✓ PostgreSQL healthy" || echo "✗ PostgreSQL unhealthy"
	@curl -sf http://localhost:8123/ping >/dev/null && \
		echo "✓ ClickHouse healthy" || echo "✗ ClickHouse unhealthy"
	@curl -sf http://localhost:9200 >/dev/null && \
		echo "✓ Elasticsearch healthy" || echo "✗ Elasticsearch unhealthy"
	@curl -sf http://localhost:8081/overview >/dev/null && \
		echo "✓ Flink JobManager healthy" || echo "✗ Flink JobManager unhealthy"
	@curl -sf http://localhost:8082/health >/dev/null && \
		echo "✓ Airflow healthy" || echo "✗ Airflow unhealthy"
	@curl -sf http://localhost:3000/api/health >/dev/null && \
		echo "✓ Grafana healthy" || echo "✗ Grafana unhealthy"
	@echo ""
	@$(MAKE) --no-print-directory pipeline-status