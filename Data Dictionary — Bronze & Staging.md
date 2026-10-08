# Data Dictionary — Bronze & Staging

Tổng hợp từ code thật trong repo (đã `dbt parse` xác nhận khớp). 3 bảng Bronze, 2 bảng Staging — chưa có bảng cho sector (mới có raw ingestion, chưa lên dbt model).

## Bronze

### `bronze_price_eod`

Nguồn: Yahoo Finance (`yfinance`, `auto_adjust=False`). Mỗi dòng là 1 ticker, 1 ngày giao dịch.

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `"Date"` | date | Ngày giao dịch |
| `"Open"` / `"High"` / `"Low"` | double | Giá mở cửa / cao nhất / thấp nhất trong ngày |
| `"Close"` | double | Giá đóng cửa — **đã được Yahoo tự điều chỉnh split sẵn**, kể cả khi `auto_adjust=False` |
| `"Adj Close"` | double | Giá đã điều chỉnh cả split lẫn cổ tức theo cách tính của Yahoo — chỉ dùng để **đối chiếu/validate** ở Silver, không dùng làm nguồn tính toán chính |
| `"Volume"` | bigint | Khối lượng giao dịch |
| `"Dividends"` | double | Cổ tức trả trong ngày đó (0 nếu không có) |
| `"Stock Splits"` | double | Hệ số chia tách trong ngày đó (0 nếu không có sự kiện) — **chỉ mang tính ghi nhận sự kiện**, không dùng để tự tính lại điều chỉnh giá vì `Close` đã điều chỉnh sẵn |
| `"Ticker"` | varchar | Mã cổ phiếu — đã chuẩn hoá hoa/thường + trim |

### `bronze_company_facts`

Nguồn: SEC EDGAR `companyfacts` API. Mỗi dòng là 1 chỉ tiêu tài chính, tại 1 kỳ cụ thể, của 1 công ty.

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `cik` | varchar | Mã định danh công ty của SEC (Central Index Key), đã chuẩn hoá |
| `tag` | varchar | Tên chỉ tiêu theo chuẩn XBRL us-gaap (vd `Assets`, `NetIncomeLoss`, `Revenues`, `EarningsPerShareBasic`, `CommonStockSharesOutstanding`, `Liabilities`) |
| `value` | double | Giá trị của chỉ tiêu |
| `unit` | varchar | Đơn vị đo (`USD`, `USD/shares`, `shares`...) |
| `form` | varchar | Loại báo cáo — chỉ còn `10-K`/`10-Q` (đã lọc ngay từ bước ingestion) |
| `fy` | integer | Năm tài chính theo khai báo của filing |
| `fp` | varchar | Kỳ tài chính (`Q1`/`Q2`/`Q3`/`FY`) theo khai báo của filing — **đây là kỳ của chính filing**, không phải kỳ thực tế của từng con số (1 filing chứa nhiều giá trị khác kỳ cùng 1 `fp`, vd quý hiện tại + lũy kế + cùng kỳ năm trước) |
| `period_start` | date, nullable | Ngày bắt đầu kỳ số liệu thực tế. `NULL` với chỉ tiêu dạng "instant" (bảng cân đối kế toán — tại 1 thời điểm, không phải khoảng thời gian) |
| `period_end` | date | Ngày kết thúc kỳ số liệu thực tế — **cùng `period_start` là cặp xác định chính xác "con số này nói về giai đoạn nào"**, dùng làm khoá dedup chính ở Silver |
| `filing_date` | date | Ngày công ty **nộp** báo cáo này lên SEC — cột cốt lõi cho point-in-time; dùng cột này để join theo thời gian, không dùng `period_end` |

### `bronze_index_membership`

Nguồn: danh sách thành viên NASDAQ-100 hiện tại. Mỗi dòng là 1 ticker.

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `"Company"` | varchar | Tên công ty |
| `"Ticker"` | varchar | Mã cổ phiếu — đã chuẩn hoá |
| `"CIK"` | varchar | Mã CIK tương ứng — dùng để join với `bronze_company_facts` |

## Staging

### `stg_price_eod`

Rename từ `bronze_price_eod` + lọc dòng giá `close_price IS NULL` hoặc `volume < 0` (rác nguồn rõ ràng).

| Cột | Kiểu | Đổi tên từ |
| --- | --- | --- |
| `trade_date` | date | `"Date"` |
| `ticker` | varchar | `"Ticker"` |
| `open_price` / `high_price` / `low_price` / `close_price` | double | `"Open"` / `"High"` / `"Low"` / `"Close"` |
| `adj_close_price` | double | `"Adj Close"` — vẫn chỉ dùng để validate |
| `volume` | bigint | `"Volume"` |
| `dividend_amount` | double | `"Dividends"` |
| `split_ratio` | double | `"Stock Splits"` — vẫn chỉ để tham chiếu |

### `stg_fundamentals`

Rename từ `bronze_company_facts` + lọc `form IN ('10-K', '10-Q')`.

| Cột | Kiểu | Đổi tên từ | Ghi chú |
| --- | --- | --- | --- |
| `cik` | varchar | `cik` | giữ nguyên |
| `metric_tag` | varchar | `tag` | chỉ `trim`, **không** `upper` — XBRL tag phân biệt hoa/thường (vd `NetIncomeLoss`) |
| `metric_value` | double | `value` |  |
| `unit_of_measure` | varchar | `unit` | trim + upper |
| `filing_type` | varchar | `form` | trim + upper, chỉ còn `10-K`/`10-Q` |
| `fiscal_year` | integer | `fy` |  |
| `fiscal_period` | varchar | `fp` | mang tính mô tả, **không dùng làm khoá dedup** |
| `period_start_date` | date, nullable | `period_start` | cùng `period_end_date` là khoá xác định kỳ thực tế |
| `period_end_date` | date | `period_end` |  |
| `filing_date` | date | `filing_date` | giữ nguyên |