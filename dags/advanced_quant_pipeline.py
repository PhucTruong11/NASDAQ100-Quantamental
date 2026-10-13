from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pathlib import Path
import os
import sys

# Đảm bảo Python container tìm thấy thư mục gốc /opt/airflow để import module ingestion
if '/opt/airflow' not in sys.path:
    sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Tiền tố môi trường cho dbt
DBT_BASE_CMD = "export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/dbt"

def fetch_universe():
    # Tải danh sách 100 mã NASDAQ-100 và ánh xạ mã CIK từ SEC.
    from ingestion.universe import NasdaqUniverse
    output_dir = "/opt/airflow/data/raw/universe"
    print(f"--> Khởi động tải NASDAQ-100 Universe vào {output_dir}...")
    universe_adapter = NasdaqUniverse(output_dir=output_dir)
    df = universe_adapter.run()
    
    if df is None or df.is_empty():
        raise ValueError("Không lấy được dữ liệu Universe từ SEC/GitHub!")
    print(f"--> Thành công! Đã ghi nhận {df.height} công ty vào universe.parquet")
    return df.height

def fetch_sector():
    # Cập nhật phân loại ngành GICS cho các mã trong Universe.
    import polars as pl
    from ingestion.adapters.sector import SectorAdapter
    universe_path = "/opt/airflow/data/raw/universe/universe.parquet"
    if not os.path.exists(universe_path):
        raise FileNotFoundError(f"Chưa có file Universe tại {universe_path}")
    df_universe = pl.read_parquet(universe_path)
    tickers = df_universe["Ticker"].to_list()
    print(f"--> Bắt đầu cập nhật ngành GICS cho {len(tickers)} tickers...")
    
    sector_adapter = SectorAdapter(output_dir="/opt/airflow/data/raw/sector")
    sector_adapter.run(tickers)
    
    sector_path = Path("/opt/airflow/data/raw/sector/sector.parquet")
    if not sector_path.exists() or sector_path.stat().st_size == 0:
        raise RuntimeError("Cập nhật phân ngành GICS thất bại: file sector.parquet không tồn tại hoặc bị rỗng!")
    print("--> Đã cập nhật xong sector.parquet!")

def verify_landing_zone():
    # Kiểm định tính toàn vẹn của các file Parquet trong Landing Zone.
    expected_files = [
        "/opt/airflow/data/raw/universe/universe.parquet",
        "/opt/airflow/data/raw/sector/sector.parquet",
    ]
    print("--> Bắt đầu kiểm tra Landing Zone...")
    for file_path in expected_files:
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"Landing zone thiếu file: {file_path}")
        size_kb = p.stat().st_size / 1024
        if size_kb == 0:
            raise ValueError(f"File bị rỗng (0 KB): {file_path}")
        print(f"  [V] {p.name}: {size_kb:.2f} KB - Hợp lệ!")
        
with DAG(
    dag_id='nasdaq100_ingestion_pipeline',
    default_args=default_args,   
    description='Cào dữ liệu tự động: Universe -> GICS Sector -> Kiểm định Landing Zone Parquet',
    schedule_interval=None,  # Kích hoạt thủ công khi test
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'parquet', 'nasdaq100'],
) as dag:
    
    # Phase 1: Ingestion
    task_universe = PythonOperator(
        task_id='fetch_nasdaq_universe',
        python_callable=fetch_universe,
    )
    
    task_sector = PythonOperator(
        task_id='fetch_gics_sector',
        python_callable=fetch_sector,
    )
    
    task_verify = PythonOperator(
        task_id='verify_landing_zone_health',
        python_callable=verify_landing_zone,
    )
    
    task_trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_pipeline',
        wait_for_completion=False, # False: Bắn tín hiệu kích hoạt xong là xong; True: Đợi DAG con chạy xong mới kết thúc
        reset_dag_run=True, # Cho phép kích hoạt chạy lại nếu DAG con đã từng chạy
    )
    
    # Luồng: Universe -> Sector -> Verify >> Tự động bật dbt pipeline!
    task_universe >> task_sector >> task_verify >> task_trigger_dbt

