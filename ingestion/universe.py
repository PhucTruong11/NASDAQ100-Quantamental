import logging
import requests
import polars as pl
from typing import List, Dict, Optional
from pathlib import Path
from datetime import date
from ingestion.identifiers import lookup_identifier, load_identifiers

logger = logging.getLogger(__name__)

class NasdaqUniverse:
    # Đường dẫn file CSV chứa danh sách mã cổ phiếu trong NASDAQ-100 được update tự động hàng ngày
    NASDAQ_100_CSV_URL = "https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv"
    
    SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
    
    def __init__(self, user_agent: str = "truongtrongphuc584@gmail.com", output_dir: str = "data/raw/universe"):
        self.user_agent = user_agent
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def get_constituents(self) -> pl.DataFrame:
        logger.info("Đang lấy danh sách CIK từ SEC...")
        
        headers = {"User-Agent": self.user_agent}
        response = requests.get(self.SEC_TICKER_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        # SEC trả về dict dạng {'0': {'cik_str': 320193, 'ticker': 'AAPL', 'title': 'Apple Inc.'}, ...}
        sec_data = response.json()
        
        nasdaq_df = pl.read_csv(self.NASDAQ_100_CSV_URL)
        live_tickers = nasdaq_df["Symbol"].to_list()
        
        extracted_data = []
        directory = {info['ticker']: info for info in sec_data.values()}
        reviewed = load_identifiers()
        # A missing identifier must never erase an index member (left join).
        for member in nasdaq_df.to_dicts():
            ticker = member['Symbol'].strip().upper()
            info = directory.get(ticker)
            fallback = lookup_identifier(ticker, date.today(), reviewed)
            extracted_data.append({
                'Company': info['title'] if info else member['Name'],
                'Ticker': ticker,
                'CIK': str(info['cik_str']) if info else (fallback['cik'] if fallback else None),
            })
                
        df = pl.DataFrame(extracted_data)
        
        # Polars: Ép kiểu cột CIK về String (Utf8)
        df = df.with_columns(pl.col("CIK").cast(pl.String))
        
        return df
    
    def save_to_parquet(self, df: pl.DataFrame) -> Path:
        # Ghi đè file universe.parquet mỗi ngày. dbt snapshot sẽ tự động capture lại các thay đổi.
        output_path = self.output_dir / "universe.parquet"
        df.write_parquet(output_path)
        return output_path
    
    def run(self) -> Optional[pl.DataFrame]:
        logger.info("Bắt đầu lấy dữ liệu danh sách NASDAQ-100 (Universe)...")
        try:
            df = self.get_constituents()
            
            if df.is_empty():
                logger.warning("Không tìm thấy dữ liệu nào cho Index membership")
                return None
                
            out_path = self.save_to_parquet(df)
            logger.info(f"Hoàn thành lấy Universe. Đã lưu tại: {out_path} (Shape: {df.shape})")
            return df
        except Exception as e:
            logger.error(f"Lỗi khi xử lý Universe: {str(e)}")
            return None

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    universe = NasdaqUniverse()
    universe.run()
