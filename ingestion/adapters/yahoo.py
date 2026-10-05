import logging
import yfinance as yf
import polars as pl
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class YahooAdapter:
    """
    Adapter để cào dữ liệu lịch sử giá cổ phiếu từ Yahoo Finance.
    Sử dụng thư viện yfinance (miễn phí, không cần API Key).
    """
    
    def __init__(self, output_dir: str = "data/raw/prices"):
        """
        Khởi tạo thư mục đầu ra.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_historical_prices(self, ticker: str, start_date: str = "2010-01-01") -> pl.DataFrame:
        # 1. Tạo object cổ phiếu
        stock = yf.Ticker(ticker)
        # 2. Lấy dữ liệu dạng Pandas DataFrame (Nhớ: auto_adjust=False)
        pdf = stock.history(start=start_date, auto_adjust=False)
        if pdf.empty:
            return pl.DataFrame()
        # 3. Kéo cột Date (đang làm index ẩn) ra ngoài thành 1 cột bình thường
        pdf = pdf.reset_index()
        # 4. Dùng lệnh pl.from_pandas() để hô biến nó thành POLARS DataFrame!
        df = pl.from_pandas(pdf)
        # 5. Dùng with_columns để tạo thêm 1 cột "Ticker" chứa mã cổ phiếu (giúp phân biệt khi gộp data sau này).
        df = df.with_columns(
            pl.lit(ticker).alias("Ticker"),
            pl.col("Date").cast(pl.Date)

        )
        return df

    def save_to_parquet(self, df: pl.DataFrame, ticker: str) -> Path:
        # Ghi trực tiếp ra file Parquet y hệt bên EDGAR
        output_path = self.output_dir / f"price_{ticker}.parquet"
        df.write_parquet(output_path)
        return output_path
        
    def run(self, ticker: str):
        logger.info(f"Bắt đầu tải giá lịch sử cho: {ticker}")
        try:
            df = self.fetch_historical_prices(ticker)
            
            if df.is_empty():
                logger.warning(f"Không lấy được giá cho {ticker}")
                return False
                
            out_path = self.save_to_parquet(df, ticker)
            logger.info(f"Hoàn thành {ticker}. Đã lưu tại: {out_path} (Shape: {df.shape})")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi xử lý {ticker}: {str(e)}")
            return False

if __name__ == "__main__":
    adapter = YahooAdapter()
    # adapter.run("MSFT")
