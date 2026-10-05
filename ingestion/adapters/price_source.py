"""
Price source adapter — yfinance / Finnhub (adapter pattern, dễ đổi nguồn).

Lưu ý (Section 16.E):
  - yfinance: không chính thức, validate schema bằng Pydantic sau mỗi fetch
  - Finnhub free tier: 60 req/phút → đếm trong sliding window
  - Chỉ ghi ra Parquet, KHÔNG mở warehouse.duckdb (Section 16.A)
  - Không redistribute dữ liệu giá thô (Section 15)
"""
