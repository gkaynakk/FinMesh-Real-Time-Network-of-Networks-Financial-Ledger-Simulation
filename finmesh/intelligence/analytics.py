from typing import Any

from core.clickhouse_writer.main import get_clickhouse_client


def get_reconciliation_summary() -> list[dict[str, Any]]:
    client = get_clickhouse_client()

    try:
        result = client.query(
            """
            SELECT
                reconciliation_status,
                count() AS trades
            FROM
            (
                SELECT
                    trade_id,
                    argMax(reconciliation_status, inserted_at)
                        AS reconciliation_status
                FROM reconciliation_results
                GROUP BY trade_id
            )
            GROUP BY reconciliation_status
            ORDER BY trades DESC
            """
        )

        return [
            {
                "reconciliation_status": row[0],
                "trades": row[1],
            }
            for row in result.result_rows
        ]

    finally:
        client.close()


def get_settlement_summary() -> list[dict[str, Any]]:
    client = get_clickhouse_client()

    try:
        result = client.query(
            """
            SELECT
                settlement_status,
                count() AS trades
            FROM
            (
                SELECT
                    trade_id,
                    argMax(settlement_status, inserted_at)
                        AS settlement_status
                FROM reconciliation_results
                GROUP BY trade_id
            )
            WHERE settlement_status IS NOT NULL
            GROUP BY settlement_status
            ORDER BY trades DESC
            """
        )

        return [
            {
                "settlement_status": row[0],
                "trades": row[1],
            }
            for row in result.result_rows
        ]

    finally:
        client.close()


def get_custody_summary() -> list[dict[str, Any]]:
    client = get_clickhouse_client()

    try:
        result = client.query(
            """
            SELECT
                custody_status,
                count() AS trades
            FROM
            (
                SELECT
                    trade_id,
                    argMax(custody_status, inserted_at)
                        AS custody_status
                FROM reconciliation_results
                GROUP BY trade_id
            )
            WHERE custody_status IS NOT NULL
            GROUP BY custody_status
            ORDER BY trades DESC
            """
        )

        return [
            {
                "custody_status": row[0],
                "trades": row[1],
            }
            for row in result.result_rows
        ]

    finally:
        client.close()