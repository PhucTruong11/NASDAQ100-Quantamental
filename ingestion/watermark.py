import json
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class WatermarkManager:
    """
    Quản lý Watermark để theo dõi trạng thái Ingestion.
    Giúp Resume/Retry lại các task bị fail mà không phải chạy lại từ đầu.
    """
    
    def __init__(self, watermark_dir: str = "data/raw/.watermarks"):
        self.watermark_dir = Path(watermark_dir)
        self.watermark_dir.mkdir(parents=True, exist_ok=True)
        
    def get_watermark(self, identifier: str) -> str:
        """Lấy ngày cập nhật cuối cùng của một mã (YYYY-MM-DD)."""
        filepath = self.watermark_dir / f"{identifier}.json"
        if filepath.exists():
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("last_updated", "")
            except Exception as e:
                logger.warning(f"Lỗi đọc watermark {identifier}: {e}")
        return ""
        
    def set_watermark(self, identifier: str):
        """Đánh dấu mã này đã được cập nhật thành công trong ngày hôm nay."""
        filepath = self.watermark_dir / f"{identifier}.json"
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({"last_updated": today}, f)
        except Exception as e:
            logger.error(f"Lỗi ghi watermark {identifier}: {e}")
            
    def is_updated_today(self, identifier: str) -> bool:
        """Kiểm tra xem mã đã được cập nhật trong ngày hôm nay chưa."""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.get_watermark(identifier) == today
