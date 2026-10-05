import logging
import requests
from bs4 import BeautifulSoup
import polars as pl
from typing import List, Dict

logger = logging.getLogger(__name__)

class NasdaqUniverse:
    """
    Class để quản lý danh sách các công ty trong rổ NASDAQ-100 (Universe).
    """
    
    # Danh sách FULL 100 mã cổ phiếu trong NASDAQ-100 (cập nhật mới nhất)
    TICKERS = [
        "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "ALAB", "ALNY", "AMAT", "AMD", 
        "AMGN", "AMZN", "APP", "ARM", "ASML", "AVGO", "AXON", "BKR", "BKNG", "CDNS", 
        "CCEP", "CSCO", "CTAS", "COST", "CPRT", "DDOG", "DXCM", "EA", "EBAY", "ENPH", 
        "EXC", "EXPE", "FAST", "FTNT", "GEHC", "GILD", "GOOG", "GOOGL", "HON", "IDXX", 
        "ILMN", "INTC", "INTU", "ISRG", "KDP", "KHC", "KLAC", "LRCX", "LULU", "MAR", 
        "MDB", "MCHP", "MDLZ", "MELI", "META", "MNST", "MRVL", "MSFT", "MU", "NFLX", 
        "NVDA", "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", 
        "PYPL", "QCOM", "REGN", "ROP", "RVLV", "SBUX", "SIRI", "SNPS", "SPLK", "STX", 
        "TEAM", "TMUS", "TSLA", "TTWO", "TXN", "VEEV", "VRSK", "VRTX", "WBA", "WBD", 
        "WDAY", "WMT", "XEL", "ZS"
    ]
    
    SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
    
    def __init__(self, user_agent: str = "truongtrongphuc584@gmail.com"):
        self.user_agent = user_agent
        
    def get_constituents(self) -> pl.DataFrame:
        """
        Lấy danh sách mã CIK từ file JSON chính thức của SEC thay vì Wikipedia để đảm bảo tính ổn định.
        :return: Polars DataFrame chứa cột [Company, Ticker, CIK]
        """
        logger.info("Đang lấy danh sách CIK từ SEC...")
        
        headers = {"User-Agent": self.user_agent}
        response = requests.get(self.SEC_TICKER_URL, headers=headers)
        response.raise_for_status()
        
        # SEC trả về dict dạng {'0': {'cik_str': 320193, 'ticker': 'AAPL', 'title': 'Apple Inc.'}, ...}
        sec_data = response.json()
        
        extracted_data = []
        for key, info in sec_data.items():
            # Chỉ lọc ra những mã nằm trong danh sách TICKERS của chúng ta
            if info["ticker"] in self.TICKERS:
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
        print(f"Tổng số công ty: {df_nasdaq.height}") # .height trong Polars = len()
