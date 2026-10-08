import logging
import time
from universe import NasdaqUniverse
from adapters.edgar import EdgarAdapter
from adapters.yahoo import YahooAdapter
from adapters.sector import SectorAdapter
from watermark import WatermarkManager

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Khởi tạo danh sách công ty (Universe)
    universe = NasdaqUniverse(user_agent="truongtrongphuc584@gmail.com")
    df_nasdaq = universe.run()
    
    if df_nasdaq is None or df_nasdaq.is_empty():
        logger.error("Không lấy được danh sách công ty. Dừng tiến trình!")
        return
        
    logger.info(f"Đã lấy thành công {df_nasdaq.height} công ty từ SEC và ghi ra parquet. Bắt đầu tải dữ liệu...")
    
    cik_list = df_nasdaq["CIK"].to_list()
    ticker_list = df_nasdaq["Ticker"].to_list()
    
    # 2. Khởi tạo các Adapters và Watermark
    edgar = EdgarAdapter(user_agent="truongtrongphuc584@gmail.com")
    yahoo = YahooAdapter()
    watermark = WatermarkManager()
    
    # 3. Vòng lặp tải dữ liệu
    for ticker, cik in zip(ticker_list, cik_list):
        if watermark.is_updated_today(ticker):
            logger.info(f"Bỏ qua {ticker} (CIK: {cik}) vì đã được cập nhật hôm nay.")
            continue
            
        logger.info(f"Đang xử lý {ticker} (CIK: {cik})...")
        try:
            yahoo_success = yahoo.run(ticker)
            edgar_success = edgar.run(cik)
            
            # Chỉ set watermark nếu cả 2 đều thành công
            if yahoo_success and edgar_success:
                watermark.set_watermark(ticker)
                logger.info(f"Đã đánh dấu watermark thành công cho {ticker}")
            else:
                logger.warning(f"Bỏ qua set watermark cho {ticker} do một trong các API thất bại hoặc rỗng.")
            # Tôn trọng rate limit của SEC
            time.sleep(0.2) 
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu cho {ticker}: {str(e)}")
            continue

    # 4. Sector (GICS-style): 1 file duy nhất, ghi đè mỗi lần chạy, không cần watermark
    sector = SectorAdapter()
    if sector.run(ticker_list):
        logger.info("Đã cập nhật sector.parquet thành công.")
    else:
        logger.warning("Cập nhật sector không hoàn tất (xem log phía trên).")

if __name__ == "__main__":
    main()
