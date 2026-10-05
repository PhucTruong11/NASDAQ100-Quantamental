import logging
import time
from universe import NasdaqUniverse
from adapters.edgar import EdgarAdapter
from adapters.yahoo import YahooAdapter

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Khởi tạo danh sách công ty (Universe)
    universe = NasdaqUniverse(user_agent="truongtrongphuc584@gmail.com")
    df_nasdaq = universe.get_constituents()
    
    if df_nasdaq.is_empty():
        logger.error("Không lấy được danh sách công ty.")
        return
        
    logger.info(f"Đã lấy thành công {df_nasdaq.height} công ty từ SEC. Bắt đầu tải dữ liệu...")
    
    cik_list = df_nasdaq["CIK"].to_list()
    ticker_list = df_nasdaq["Ticker"].to_list()
    
    # 2. Khởi tạo các Adapters
    edgar = EdgarAdapter(user_agent="truongtrongphuc584@gmail.com")
    yahoo = YahooAdapter()
    
    # 3. Vòng lặp tải dữ liệu
    for ticker, cik in zip(ticker_list, cik_list):
        logger.info(f"Đang xử lý {ticker} (CIK: {cik})...")
        try:
            # Lấy dữ liệu Giá (Price)
            yahoo.run(ticker)
            
            # Lấy dữ liệu Báo cáo Tài chính (Fundamentals)
            edgar.run(cik)
            
            # Tôn trọng rate limit của SEC
            time.sleep(0.2) 
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu cho {ticker}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
