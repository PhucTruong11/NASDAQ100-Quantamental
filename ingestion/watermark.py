"""
Watermark — theo dõi checkpoint để ingestion incremental.

Ghi watermark ra file JSON trong data/raw/.watermarks/
để các task Airflow có thể đọc lại khi retry mà không fetch lại từ đầu.
"""
