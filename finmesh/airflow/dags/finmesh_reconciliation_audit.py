from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="finmesh_reconciliation_audit",
    description="Audits FinMesh reconciliation results in ClickHouse",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    tags=["finmesh", "reconciliation", "data-quality"],
) as dag:

    check_reconciliation_data = BashOperator(
        task_id="check_reconciliation_data",
        bash_command="""
        python - <<'PY'
import urllib.parse
import urllib.request

query = '''
SELECT count()
FROM finmesh.reconciliation_latest
'''

url = (
    'http://clickhouse:8123/?'
    + urllib.parse.urlencode({
        'user': 'finmesh',
        'password': 'finmesh',
        'query': query
    })
)

result = urllib.request.urlopen(url, timeout=10).read().decode().strip()
count = int(result)

print(f'Reconciliation rows: {count}')

if count == 0:
    raise RuntimeError('No reconciliation data found')
PY
        """,
    )

    check_invalid_statuses = BashOperator(
        task_id="check_invalid_statuses",
        bash_command="""
        python - <<'PY'
import urllib.parse
import urllib.request

query = '''
SELECT count()
FROM finmesh.reconciliation_latest
WHERE reconciliation_status NOT IN
(
    'CONSISTENT',
    'PENDING',
    'SETTLEMENT_FAILED',
    'CUSTODY_BLOCKED'
)
'''

url = (
    'http://clickhouse:8123/?'
    + urllib.parse.urlencode({
        'user': 'finmesh',
        'password': 'finmesh',
        'query': query
    })
)

result = urllib.request.urlopen(url, timeout=10).read().decode().strip()
invalid = int(result)

print(f'Invalid reconciliation statuses: {invalid}')

if invalid > 0:
    raise RuntimeError(
        f'Found {invalid} invalid reconciliation statuses'
    )
PY
        """,
    )

    reconciliation_summary = BashOperator(
        task_id="reconciliation_summary",
        bash_command="""
        python - <<'PY'
import urllib.parse
import urllib.request

query = '''
SELECT
    reconciliation_status,
    count() AS total
FROM finmesh.reconciliation_latest
GROUP BY reconciliation_status
ORDER BY total DESC
FORMAT PrettyCompact
'''

url = (
    'http://clickhouse:8123/?'
    + urllib.parse.urlencode({
        'user': 'finmesh',
        'password': 'finmesh',
        'query': query
    })
)

result = urllib.request.urlopen(url, timeout=10).read().decode()

print('FinMesh Reconciliation Summary')
print(result)
PY
        """,
    )

    check_reconciliation_data >> check_invalid_statuses >> reconciliation_summary