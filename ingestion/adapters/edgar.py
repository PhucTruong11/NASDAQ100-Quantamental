import logging
import requests
import polars as pl
from pathlib import Path
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

# Cấu hình logging cơ bản
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EdgarFact(BaseModel):
    cik: str
    tag: str
    value: float
    unit: str
    form: str
    fy: Optional[int] = None
    fp: Optional[str] = None
    filing_date: str


class EdgarAdapter:
    """
    Adapter để lấy và xử lý dữ liệu BCTC từ SEC EDGAR API.
    Nguồn: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
    """
    
    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
    
    def __init__(self, user_agent: str, output_dir: str = "data/raw/edgar"):
        """
        Khởi tạo adapter.
        :param user_agent: SEC bắt buộc phải có User-Agent chuẩn (VD: "Your Name your@email.com")
        :param output_dir: Thư mục lưu file parquet đầu ra.
        """
        self.user_agent = user_agent
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate"
        }
        
        # Danh sách các thẻ (tags) us-gaap quan trọng cần lấy cho mô hình Quantamental
        self.target_tags = [
            "Assets",                   # Tổng tài sản
            "Liabilities",              # Tổng nợ
            "NetIncomeLoss",            # Lợi nhuận ròng
            "Revenues",                 # Doanh thu (hoặc SalesRevenueNet)
            "EarningsPerShareBasic",    # EPS
            "CommonStockSharesOutstanding" # Số lượng cổ phiếu lưu hành
        ]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_company_facts(self, cik: str) -> dict:
        # 1. Pad mã CIK cho đủ 10 số bằng hàm zfill của Python
        cik_padded = str(cik).zfill(10)

        # 2. Xây dựng URL hoàn chỉnh.
        url = f"{self.BASE_URL}/CIK{cik_padded}.json"
        # 3. Gửi GET request với self.headers.
        response = requests.get(url, headers=self.headers)
        
        # 4. Kiểm tra status code (nếu 200 thì trả về json, nếu lỗi thì log và raise Exception).
        response.raise_for_status()
        return response.json()
        

    def parse_facts(self, raw_data: dict, cik: str) -> pl.DataFrame:
        extracted_data = []
        us_gaap = raw_data.get("facts", {}).get("us-gaap", {})
        
        for tag in self.target_tags:
            if tag in us_gaap:
                tag_data = us_gaap[tag]
                units = tag_data.get("units", {})
                
                for unit_name, facts in units.items():
                    for fact in facts:
                        # Chỉ lấy báo cáo năm (10-K) và báo cáo quý (10-Q)
                        if fact.get("form") in ["10-K", "10-Q"]:
                            try:
                                # Validate bằng Pydantic
                                validated_fact = EdgarFact(
                                    cik=cik,
                                    tag=tag,
                                    value=fact.get("val"),
                                    unit=unit_name,
                                    form=fact.get("form"),
                                    fy=fact.get("fy"),
                                    fp=fact.get("fp"),
                                    filing_date=fact.get("filed")
                                )
                                extracted_data.append(validated_fact.model_dump())
                            except ValidationError as e:
                                logger.debug(f"Bỏ qua dữ liệu không hợp lệ của {cik} - {tag}: {e}")
                                continue
        # 1. Tạo DataFrame từ list of dicts. Tốc độ ngang ngửa Pandas.  
        df = pl.DataFrame(extracted_data)
        
        # 2. Xử lý Dataframe
        # pl.col() đại diện cho 1 cột. Ở đây ta cast (ép kiểu) filing_date thành Datetime và value thành Float64
        if not df.is_empty():
            df = df.with_columns(
                pl.col("filing_date").cast(pl.Date),
                pl.col("value").cast(pl.Float64)
            )
        return df   

    def save_to_parquet(self, df: pl.DataFrame, cik: str) -> Path:
        output_path = self.output_dir / f"edgar_{cik}.parquet"
        # Ghi trực tiếp ra parquet
        df.write_parquet(output_path)
        return output_path

    def run(self, cik: str):
        logger.info(f"Bắt đầu lấy dữ liệu EDGAR cho CIK: {cik}")
        try:
            raw_data = self.fetch_company_facts(cik)
            df = self.parse_facts(raw_data, cik)
            if df.is_empty():
                logger.warning(f"Không tìm thấy dữ liệu nào cho CIK {cik}")
                return None
            out_path = self.save_to_parquet(df,cik)
            logger.info(f"Hoàn thành CIK {cik}. Đã lưu tại: {out_path} (Số dòng/cột: {df.shape})")
        except Exception as e:
            logger.error(f"Lỗi khi xử lý CIK {cik}: {str(e)}")

if __name__ == "__main__":
    adapter = EdgarAdapter(user_agent="truongtrongphuc584@gmail.com")
    adapter.run("320193") # 320193 là Apple
