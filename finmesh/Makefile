.PHONY: up down bank validator exchange settlement custody reconciliation clickhouse ledger verify

up:
	docker compose up -d

down:
	docker compose down

bank:
	python -m networks.bank_producer.main

validator:
	python -m core.validator_service.main

exchange:
	python -m networks.exchange_network.main

settlement:
	python -m networks.settlement_network.main

custody:
	python -m networks.custody_network.main

reconciliation:
	python -m core.reconciliation_service.main

clickhouse:
	python -m core.clickhouse_writer.main

ledger:
	python -m core.ledger_writer.main

verify:
	python -m core.ledger_verifier.main