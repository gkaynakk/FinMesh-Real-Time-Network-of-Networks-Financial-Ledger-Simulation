from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="finmesh_health_audit",
    description="Checks core FinMesh infrastructure health",
    start_date=datetime(2026, 8, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["finmesh", "health"],
) as dag:

    check_redpanda = BashOperator(
        task_id="check_redpanda",
        bash_command=(
            "python - <<'PY'\n"
            "import urllib.request\n"
            "urllib.request.urlopen('http://redpanda:9644/v1/status/ready', timeout=5)\n"
            "print('Redpanda healthy')\n"
            "PY"
        ),
    )

    check_flink = BashOperator(
        task_id="check_flink",
        bash_command=(
            "python - <<'PY'\n"
            "import urllib.request\n"
            "urllib.request.urlopen('http://flink-jobmanager:8081/overview', timeout=5)\n"
            "print('Flink healthy')\n"
            "PY"
        ),
    )

    check_clickhouse = BashOperator(
        task_id="check_clickhouse",
        bash_command=(
            "python - <<'PY'\n"
            "import urllib.request\n"
            "response = urllib.request.urlopen('http://clickhouse:8123/ping', timeout=5)\n"
            "print(response.read().decode().strip())\n"
            "PY"
        ),
    )

    check_elasticsearch = BashOperator(
        task_id="check_elasticsearch",
        bash_command=(
            "python - <<'PY'\n"
            "import urllib.request\n"
            "urllib.request.urlopen('http://elasticsearch:9200', timeout=5)\n"
            "print('Elasticsearch healthy')\n"
            "PY"
        ),
    )

    check_redpanda >> check_flink >> check_clickhouse >> check_elasticsearch