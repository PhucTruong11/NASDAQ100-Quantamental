from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'pool': 'duckdb_access',
    'append_env': True,
    'env': {
        'QUANT_DBT_VARS': '{{ {"allow_observed_universe": params.allow_observed_universe} | tojson }}',
    },
}

# Tiền tố môi trường cho dbt
DBT_BASE_CMD = "export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/dbt"
DBT_VARS = ' --vars "$QUANT_DBT_VARS"'

with DAG(
    dag_id='dbt_transformation_pipeline',
    default_args=default_args,
    description='Bronze -> Silver -> Intermediate -> Gold/coverage -> Quality gates -> Input audit',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'allow_observed_universe': Param(
            False, type='boolean',
            description='Research only: allow monthly/live observations; these are not effective-date history.',
        ),
    },
    tags=['dbt', 'transformation', 'nasdaq100', 'medallion'],
) as dag:

    # Tầng Bronze: Nạp Parquet từ data/raw vào DuckDB
    task_dbt_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=f"{DBT_BASE_CMD} && dbt run --select bronze --no-partial-parse" + DBT_VARS,
    )

    # Tầng Staging & Silver: Làm sạch, ép kiểu & Khử trùng lặp Point-in-time
    task_dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(f"{DBT_BASE_CMD} && dbt seed --select universe_membership_history" + DBT_VARS
                      + " && dbt snapshot --select silver_universe_membership" + DBT_VARS
                      + " && dbt run --select staging silver --no-partial-parse" + DBT_VARS),
    )

    # Intermediate: Tính toán 6 Factor Định lượng & Z-Scores
    task_dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f"{DBT_BASE_CMD} && dbt build --select intermediate --no-partial-parse" + DBT_VARS,
    )

    # Cổng kiểm soát chất lượng dữ liệu: structural + financial semantics tests
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_gates',
        bash_command=f"{DBT_BASE_CMD} && dbt test --no-partial-parse" + DBT_VARS,
    )

    task_dbt_gold = BashOperator(
        task_id='dbt_build_gold',
        # on-run-end writes coverage after all five Gold models have built.
        bash_command=f"{DBT_BASE_CMD} && dbt build --select gold --no-partial-parse" + DBT_VARS,
    )

    task_audit_inputs = BashOperator(
        task_id='audit_backtest_inputs',
        bash_command='cd /opt/airflow && python -m research.backtest_quality',
    )

    task_dbt_bronze >> task_dbt_silver >> task_dbt_intermediate >> task_dbt_gold >> task_dbt_test >> task_audit_inputs
