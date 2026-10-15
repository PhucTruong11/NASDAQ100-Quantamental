from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def query_duckdb_warehouse():
    import duckdb
    db_path = '/opt/airflow/data/warehouse.duckdb'
    print(f"--> Đang kiểm tra sự tồn tại của file: {db_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Không tìm thấy file database tại {db_path}!")
    print(f"-->  File tồn tại! Dung lượng: {os.path.getsize(db_path) / (1024*1024):.2f} MB")
    
    # BẮT BUỘC: read_only=True để an toàn tuyệt đối, tránh tranh chấp với IDE/dbt
    con = duckdb.connect(db_path, read_only=True)
    
    # Kiểm tra danh sách bảng Intermediate vừa hoàn thiện
    print("\n Kiểm tra các bảng intermediate trong warehouse")
    tables_df = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'int_%'
        ORDER BY table_name                       
    """).fetchdf()
    print(tables_df.to_string(index=False))
        
    # Đếm số dòng của từng Factor
    print("\n thống kê số dòng FACTOR")
    counts_query = """
        SELECT
            (SELECT COUNT(*) FROM int_rebalance_universe) AS universe_rows,
            (SELECT COUNT(*) FROM int_momentum_ratio)    AS momentum_rows,
            (SELECT COUNT(*) FROM int_growth_ratio)      AS growth_rows,
            (SELECT COUNT(*) FROM int_fcf_yield)         AS fcf_yield_rows,
            (SELECT COUNT(*) FROM int_sbc_ratio)         AS sbc_ratio_rows,
            (SELECT COUNT(*) FROM int_earnings_surprise) AS earnings_surprise_rows
    """
    count_df = con.execute(counts_query).fetchdf()
    print(count_df.to_string(index=False))
        
    con.close()
    return "Kết nối và truy vấn DuckDB thành công 100%!"

with DAG(
    dag_id='test_duckdb_connect',
    default_args=default_args,
    description='Kiểm tra khả năng kết nối và đọc DuckDB từ Airflow container',
    schedule_interval = None, # Chạy bằng tay khi cần test
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tests', 'duckdb', 'intermediate'],
) as dag:
    
    task_check_duckdb = PythonOperator(
        task_id='task_query_duckdb_warehouse',
        pool='duckdb_access',
        python_callable=query_duckdb_warehouse,
    )
