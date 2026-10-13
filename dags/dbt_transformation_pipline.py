from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Tiền tố môi trường cho dbt
DBT_BASE_CMD = "export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/dbt"

with DAG(
    dag_id='dbt_transformation_pipeline',
    default_args=default_args,
    description='Tự động hóa dbt build: Bronze -> Staging/Silver -> Intermediate Factors -> Data Quality Gates',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'nasdaq100', 'medallion'],
) as dag:
    
     # Tầng Bronze: Nạp Parquet từ data/raw vào DuckDB
    task_dbt_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=f"{DBT_BASE_CMD} && dbt run --select bronze --no-partial-parse",
    )
    
    # Tầng Staging & Silver: Làm sạch, ép kiểu & Khử trùng lặp Point-in-time
    task_dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f"{DBT_BASE_CMD} && dbt run --select staging silver --no-partial-parse",
    ) 
    
    # Intermediate: Tính toán 6 Factor Định lượng & Z-Scores
    task_dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f"{DBT_BASE_CMD} && dbt run --select intermediate --no-partial-parse",
    )
    
    # Cổng kiểm soát chất lượng dữ liệu: Toàn bộ 42 bài Data Tests
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_gates',
        bash_command=f"{DBT_BASE_CMD} && dbt test --no-partial-parse",
    )
    
    # Định nghĩa chuỗi phụ thuộc tuần tự 4 chặng Medallion
    task_dbt_bronze >> task_dbt_silver >> task_dbt_intermediate >> task_dbt_test  