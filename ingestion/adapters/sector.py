import logging
import os
import sys
import time
import polars as pl
import yfinance as yf
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class SectorRecord(BaseModel):
    ticker: str
    sector: str
    industry: Optional[str] = None


class SectorAdapter:
    """
    Adapter lấy phân loại Sector/Industry cho từng ticker từ Yahoo Finance (yfinance .info).
    Kết quả ghi đè vào 1 file duy nhất: data/raw/sector/sector.parquet (giống universe.py).

    Giới hạn đã biết và chấp nhận (phiên bản đầu):
    - Sector là phân loại HIỆN TẠI của Yahoo, áp cho toàn bộ lịch sử (không point-in-time).
      Sector hiếm khi đổi nên sai lệch nhỏ, nhưng về lý thuyết có look-ahead nhẹ.
    - Phân loại kiểu GICS của Yahoo (vd "Technology"), không phải bản GICS chính thức của MSCI/S&P.
    - Chưa có nguồn dự phòng SEC SIC (sicDescription); chỉ thêm nếu yfinance .info bị chặn/không ổn định.
    """

    OUTPUT_FILE = "sector.parquet"
    SCHEMA = {"ticker": pl.String, "sector": pl.String, "industry": pl.String}

    def __init__(
        self,
        output_dir: str = "data/raw/sector",
        min_success_ratio: float = 0.9,
        request_delay: float = 0.2,
    ):
        """
        :param output_dir: Thư mục chứa sector.parquet.
        :param min_success_ratio: Tỉ lệ ticker tối thiểu lấy được sector để ghi đè file.
            Dưới ngưỡng này thì giữ nguyên file cũ, tránh việc 1 lần Yahoo chặn làm mất dữ liệu tốt.
        :param request_delay: Giây nghỉ giữa các ticker để tránh bị Yahoo rate limit.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.min_success_ratio = min_success_ratio
        self.request_delay = request_delay

    @staticmethod
    def _normalize_ticker(ticker: str) -> str:
        # Chuẩn hoá giống toàn project: upper + trim
        return str(ticker).strip().upper()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def fetch_sector_info(self, ticker: str) -> dict:
        # Yahoo dùng "-" thay cho "." trong class share (vd BRK.B -> BRK-B)
        info = yf.Ticker(ticker.replace(".", "-")).info
        # yfinance đôi khi trả dict rỗng/gần rỗng khi bị chặn -> coi là lỗi tạm thời để retry
        if not info or len(info) <= 1:
            raise RuntimeError(f"yfinance trả về info rỗng cho {ticker}")
        return info

    def fetch_sectors(self, tickers: List[str]) -> pl.DataFrame:
        records, failed = [], []

        for raw_ticker in tickers:
            ticker = self._normalize_ticker(raw_ticker)
            try:
                info = self.fetch_sector_info(ticker)
                record = SectorRecord(
                    ticker=ticker,
                    sector=info.get("sector"),
                    industry=info.get("industry"),
                )
                records.append(record.model_dump())
            except ValidationError:
                logger.warning(f"Bỏ qua {ticker}: Yahoo không có sector hợp lệ")
                failed.append(ticker)
            except Exception as e:
                logger.error(f"Lỗi khi lấy sector cho {ticker}: {str(e)}")
                failed.append(ticker)

            time.sleep(self.request_delay)

        self.failed_tickers = failed
        return pl.DataFrame(records, schema=self.SCHEMA)

    def save_to_parquet(self, df: pl.DataFrame) -> Path:
        # Ghi đè sector.parquet mỗi lần chạy. Ghi ra file tạm rồi replace để không để lại file hỏng nửa chừng.
        output_path = self.output_dir / self.OUTPUT_FILE
        tmp_path = output_path.with_suffix(".parquet.tmp")
        df.write_parquet(tmp_path)
        os.replace(tmp_path, output_path)
        return output_path

    def run(self, tickers: List[str]) -> bool:
        """
        Trả về True chỉ khi file đã được ghi VÀ mọi ticker đều lấy được sector.
        Trả về False nếu có ticker lỗi (file vẫn được ghi nếu đạt min_success_ratio)
        hoặc nếu không ghi được file.
        """
        logger.info(f"Bắt đầu lấy sector cho {len(tickers)} ticker...")
        self.failed_tickers = []
        try:
            if not tickers:
                logger.warning("Danh sách ticker rỗng, không có gì để lấy sector")
                return False

            df = self.fetch_sectors(tickers)
            total = len(tickers)
            ratio = df.height / total

            if ratio < self.min_success_ratio:
                logger.error(
                    f"Chỉ lấy được {df.height}/{total} ticker (< {self.min_success_ratio:.0%}). "
                    f"Giữ nguyên file cũ, không ghi đè. Lỗi: {self.failed_tickers}"
                )
                return False

            out_path = self.save_to_parquet(df)
            if self.failed_tickers:
                logger.warning(
                    f"Đã lưu {out_path} (Shape: {df.shape}) nhưng thiếu {len(self.failed_tickers)} ticker: {self.failed_tickers}"
                )
                return False

            logger.info(f"Hoàn thành lấy Sector. Đã lưu tại: {out_path} (Shape: {df.shape})")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi xử lý Sector: {str(e)}")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Dùng: python ingestion/adapters/sector.py            -> full universe, ghi data/raw/sector/sector.parquet
    #       python ingestion/adapters/sector.py AAPL MSFT  -> test nhanh, ghi data/raw/sector_test/ (không đụng file thật)
    args = sys.argv[1:]
    if args:
        adapter = SectorAdapter(output_dir="data/raw/sector_test")
        ok = adapter.run(args)
    else:
        universe_path = Path("data/raw/universe/universe.parquet")
        tickers = pl.read_parquet(universe_path)["Ticker"].to_list()
        adapter = SectorAdapter()
        ok = adapter.run(tickers)
    sys.exit(0 if ok else 1)
