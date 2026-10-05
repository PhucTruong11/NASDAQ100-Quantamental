import logging
import requests
import polars as pl
from typing import List, Dict

logger = logging.getLogger(__name__)

class NasdaqUniverse:
    # Đường dẫn file CSV chứa danh sách mã cổ phiếu trong NASDAQ-100 được update tự động hàng ngày
    NASDAQ_100_CSV_URL = "https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv"
    
    SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
    
    def __init__(self, user_agent: str = "truongtrongphuc584@gmail.com"):
        self.user_agent = user_agent
        
    def get_constituents(self) -> pl.DataFrame:
        logger.info("Đang lấy danh sách CIK từ SEC...")
        
        headers = {"User-Agent": self.user_agent}
        response = requests.get(self.SEC_TICKER_URL, headers=headers)
        response.raise_for_status()
        
        # SEC trả về dict dạng {'0': {'cik_str': 320193, 'ticker': 'AAPL', 'title': 'Apple Inc.'}, ...}
        sec_data = response.json()
        
        nasdaq_df = pl.read_csv(self.NASDAQ_100_CSV_URL)
        live_tickers = nasdaq_df["Symbol"].to_list()
        
        extracted_data = []
        for key, info in sec_data.items():
            if info["ticker"] in live_tickers:
                extracted_data.append({
                    "Company": info["title"],
                    "Ticker": info["ticker"],
                    "CIK": str(info["cik_str"])
                })
                
        df = pl.DataFrame(extracted_data)
        
        # Polars: Ép kiểu cột CIK về String (Utf8)
        df = df.with_columns(pl.col("CIK").cast(pl.String))
        
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    universe = NasdaqUniverse()
    df_nasdaq = universe.get_constituents()
    
    if df_nasdaq is not None:
        print(df_nasdaq.head())
        print(f"Tổng số công ty: {df_nasdaq.height}")
