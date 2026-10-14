# Fundamentals, FCF và universe — quy ước hiện hành

## Fundamentals

- `silver_fundamentals_pit` giữ **bản công bố đầu tiên** cho từng CIK/tag/unit/start/end. Đây là chính sách first-reported, không phải latest-restated-as-of. Không thay nó bằng dữ liệu restatement lấy từ tương lai.
- `filing_date` được giữ để audit; `available_date = filing_date + 1 ngày`. Companyfacts không có acceptance timestamp trong schema hiện tại, nên không dùng filing trong danh mục hình thành tại close cùng ngày.
- `silver_fundamentals_annual` chỉ lấy flow có đơn vị `USD`, duration 330–400 ngày. `fy/fp = FY` không quyết định một fact là cả năm. Kỳ chuyển tiếp ngắn bị loại; 52/53-week years được giữ. Đây là annual, **chưa phải rolling TTM**.
- Growth ghép Net Income với Revenue theo cùng **start/end**. Doanh thu năm trước phải có cả start/end cách năm hiện tại 330–400 ngày và đã available tại rebalance; không nhảy qua năm thiếu dữ liệu.
- SBC cũng ghép cùng kỳ Revenue. Earnings growth theo quý chỉ nhận USD.
- NULL là thiếu dữ liệu, không phải 0. Z-score thiếu dispersion hoặc thiếu sector giữ NULL.

## FCF Yield

- `FCF = OCF - abs(Capex)` với OCF/Capex cùng annual USD period. Thiếu Capex → FCF NULL; Capex được báo cáo bằng 0 → giữ 0.
- Giá tính return (`adjusted_close_price`) được giữ. `nominal_close_price` khôi phục giá danh nghĩa bằng cách đảo các split **sau ngày giá**, vì Yahoo Close đã split-adjusted đến ngày tải dữ liệu. Không dùng giá điều chỉnh cổ tức để tính vốn hóa.
- Shares dùng instant fact, unit `SHARES`, available trước rebalance. Nhân split factor giữa ngày đo shares và rebalance để shares và giá danh nghĩa cùng cơ sở.
- Khi cùng CIK có nhiều ticker trong universe tại kỳ đó (ví dụ GOOG/GOOGL), FCF Yield giữ NULL: issuer-level shares không đủ để định giá từng share class. Chưa hỗ trợ class-level market cap hoặc buyback/issuance chưa được công bố giữa hai kỳ báo cáo.

## Universe

`dbt/seeds/universe_membership_history.csv` có half-open windows `[valid_from, valid_to)` cùng source URL và hai nhãn chất lượng:

- `effective`: ngày hiệu lực đã xác minh từ nguồn lịch sử; được dùng mặc định.
- `monthly_observation`: ảnh chụp archive, chỉ dùng khi chủ động bật `allow_observed_universe: true`.
- `identifier_quality`: `current_mapping` là mapping từ universe hiện tại, chưa xác minh lịch sử ticker reuse; `unmapped` giữ CIK NULL. Không loại ticker khỏi membership chỉ vì thiếu CIK/giá/sector.

Seed hiện có 40 ảnh chụp yfiua, 4.040 dòng từ **31/07/2023**. Mỗi ảnh dùng phiên bản commit công bố đầu tiên và chỉ available từ ngày kế tiếp; không backdate theo tên thư mục tháng. Nguồn là [yfiua/index-constituents](https://github.com/yfiua/index-constituents), giấy phép mã nguồn [Apache 2.0](https://github.com/yfiua/index-constituents/blob/main/LICENSE); các URL commit của từng ảnh được lưu trong seed.

Ảnh chụp không cung cấp ngày hiệu lực thay đổi giữa tháng. Không dùng seed này để tuyên bố đã có lịch sử NASDAQ-100 PIT chính xác từ 2010. Một observation chỉ được carry forward đến ảnh kế tiếp, tối đa 40 ngày, và seed hiện tại chỉ phủ đến 14/10/2026 inclusive.

`silver_universe_membership_pit` kết hợp seed với live dbt snapshot từ ngày capture trở đi. `int_rebalance_universe` luôn kiểm tra cả hai đầu interval, bỏ tháng chưa kết thúc. Kỳ trước coverage bị loại; nếu không có kỳ nào phù hợp, dbt quality gate báo lỗi rõ ràng.

Momentum lấy các offset **tháng lịch** từ toàn bộ giá, không LAG trên các spell membership bị đứt quãng. Điều này cho phép cổ phiếu mới vào index sử dụng lịch sử giá trước khi gia nhập.

### Chạy nghiên cứu với dữ liệu observation

Từ root:

```powershell
python -m unittest discover -s tests -v
python -m ingestion.main
```

Từ thư mục `dbt` (profile hiện tại):

```powershell
dbt build --select +int_growth_ratio +int_fcf_yield +int_sbc_ratio +int_earnings_surprise +int_momentum_ratio --vars '{allow_observed_universe: true}'
```

Airflow transformation DAG có param `allow_observed_universe`, mặc định `false`. Muốn chạy nghiên cứu bằng archive, trigger DAG với param `true`. DAG đã chạy seed và snapshot trước Silver. Ingestion DAG hiện chỉ fetch universe/sector; vẫn cần chạy ingestion giá/EDGAR nêu trên.

`assert_universe_input_coverage` WARN khi thiếu CIK, giá hoặc sector; không thu nhỏ universe để làm warning biến mất. Các ticker lịch sử chưa có identifier/giá phải được bổ sung hoặc ghi rõ exclusion trước khi diễn giải performance. Sector hiện tại vẫn chưa PIT.

### Refresh archive và nhập lịch sử chính xác

```powershell
python -m ingestion.universe_history --through 2026-10-14
python -m ingestion.universe_history --import-csv path/to/verified-membership.csv
```

Archive downloader cache dưới `data/raw/universe_archive/`; nếu GitHub unauthenticated API bị rate-limit, có thể cấu hình `GITHUB_TOKEN` trong môi trường (không commit token). Nó giữ file cũ nếu tải không hoàn chỉnh và từ chối đè một seed đã chứa `effective` bằng observation.

CSV import cần đúng header của seed; ngày ISO, `valid_to` bắt buộc, provenance HTTPS, không duplicate/overlap. Nguồn effective phải bảo vệ được ngày hiệu lực và CIK theo lịch sử; việc gán nhãn thủ công không tự xác minh nguồn.

## Kiểm chứng

Tests dùng SQL production trên fixture in-memory: quý gắn FY, currency khác USD, 53-week year, comparative công bố sau rebalance, filing cùng ngày, Capex thiếu/khác kỳ/zero, split trước/sau rebalance, nhiều share class, membership boundaries, observation opt-in, momentum trước membership và z-score thiếu dispersion.

dbt integration được chạy trên **bản sao warehouse**, không ghi đè warehouse đang nghiên cứu. Chế độ strict phải fail khi seed chỉ có observations; đó là quality gate dự kiến, không phải pipeline đã có dữ liệu effective-date.

Kết quả kiểm chứng ngày 14/10/2026: 13 fixture tests pass; build các factor cùng upstream có 19 node success, 51 data tests pass, 1 coverage warning (426 ticker-kỳ). Universe nghiên cứu có 3.939 dòng, 124 ticker, các kỳ hoàn tất từ 31/07/2023 đến 30/09/2026. Kiểm tra strict với `allow_observed_universe: false` đã fail đúng gate không có verified coverage. Không có kết quả performance/backtest trong lần kiểm chứng này.
