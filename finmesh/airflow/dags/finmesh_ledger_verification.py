from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="finmesh_ledger_verification",
    description="Verifies the FinMesh hash-chained ledger integrity",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    tags=["finmesh", "ledger", "integrity"],
) as dag:

    verify_ledger = BashOperator(
        task_id="verify_ledger_chain",
        bash_command="""
        cd /opt/finmesh &&
        python -m core.ledger_verifier.main
        """,
    )