# Chốt dữ liệu cho backtest — 15/10/2026

## Kết luận sử dụng

Pipeline hiện đủ để dựng **backtest nghiên cứu trên mẫu signal đã lọc**. Chưa đủ
để tuyên bố hiệu quả đại diện toàn NASDAQ-100: membership là monthly observations,
sector là metadata hiện tại và giá của một số mã delisted chưa đầy đủ. Không đổi
nhãn các nguồn này thành verified PIT và không loại mã bằng thông tin return tương lai.

## Bằng chứng trước/sau

Baseline là warehouse trước remediation, cùng 3.939 ticker–kỳ / 39 kỳ hoàn tất.
Kỳ mới nhất: 30/09/2026, denominator 101 mã.

| Chỉ tiêu | Trước | Sau nạp bổ sung | Cuối cùng, freshness trước z-score |
| --- | ---: | ---: | ---: |
| Value có score, kỳ gần nhất | 51 | 57 | 52 |
| Composite có đủ 4 pillar, kỳ gần nhất | 49 | 54 | 48 |
| Composite có đủ 4 pillar, toàn lịch sử | 1.715 | 2.094 | 1.819 |
| Thiếu CIK, ticker–kỳ | 426 | 0 | 0 |
| Thiếu sector trong grid, ticker–kỳ | 426 | 49 | 49 |
| Thiếu giá đúng rebalance, ticker–kỳ | 127 | 127 | 127 |
| Warning input coverage, ticker–kỳ (các loại có thể chồng nhau) | 465 | 127 | 127 |

Cuối cùng **48/101 mã** có signal đủ điều kiện nghiên cứu ở kỳ mới nhất. Tuổi input
được chặn ngay trong lựa chọn fact ở Intermediate, **trước z-score**: input cũ không
được tham gia mean/stddev của các mã còn lại. Audit kiểm tra lại cùng policy sau Gold.
Vì policy đã chặt hơn baseline, số mã cuối kỳ có thể ít hơn dù sửa thêm được dữ liệu.

## Nguyên nhân và xử lý

| Finding | Bằng chứng / rủi ro | Xử lý | Mức / độ chắc chắn |
| --- | --- | --- | --- |
| Shares bị bỏ sót do namespace | Adapter trước chỉ đọc us-gaap; SEC còn có dei:EntityCommonStockSharesOutstanding. MDLZ là ví dụ thiếu GAAP shares nhưng có DEI shares. | Đọc DEI riêng, giữ tag qualified, dùng instant shares đã available; giữ guard nhiều classes. | High / đã đối chiếu SEC và test |
| Thiếu CIK của mã rời index | History lookup chỉ dựa trên current NASDAQ subset; 24 ticker không có mapping. | Registry có nguồn SEC và window được rà soát; điền CIK thiếu, giữ nguyên membership dates/quality. | High / đã đối chiếu issuer names và SEC sources |
| Sector bị mất khi refresh current members | Raw sector chỉ còn 100 mã; 426 ticker–kỳ trong grid thiếu sector. | Backfill các ticker lịch sử lấy được; refresh merge để giữ các dòng cũ. Còn ANSS/SGEN/SPLK/WBA thiếu sector. | High / đã đối chiếu bảng và provider responses |
| Filing-form filter quá hẹp | US-GAAP USD facts trong 20-F/40-F bị loại cùng các foreign forms. | Cho phép 20-F/40-F, vẫn giữ namespace/concept, USD và duration checks. Chưa map IFRS hay tự đổi tiền tệ. | Medium / đã kiểm tra parser và source profile |
| Giá bị thiếu cả lịch sử | EA/WBD chỉ có các điểm rời rạc; Yahoo không trả history của ANSS/WBA/SGEN/SPLK. | Retry có ghi kết quả; giữ NULL. Thêm guard không cho refresh làm rơi ngày đã có trong file cũ. | High / đã kiểm tra provider và grid |
| Input cũ vẫn tạo ra score | Sau nạp bổ sung, 6/54 mã đủ 4 pillars dùng ít nhất một input quá tuổi policy. | Chặn fact quá tuổi trước z-score, giữ rule thiếu score đúng; audit kiểm tra thêm is_research_signal_eligible. | High / kiểm tra theo ngày thực tế; thresholds là policy nghiên cứu |

Registry `config/historical_identifiers.csv` ghi CIK, issuer, window và SEC source.
Window ở đây là phạm vi membership được cross-check, không phải ngày hiệu lực corporate
action hay chứng cứ thành phần index. Mapping không được tự ghi đè CIK đang khác.
Unknown CIK không còn làm mất dòng khỏi live universe (left join).

## Vì sao vẫn thiếu Value?

Giữ định nghĩa FCF hiện tại: **annual USD OCF toàn entity − PPE Capex cùng kỳ**, chia
market cap theo policy shares/price hiện có. Không mở rộng định nghĩa để tăng số mẫu.

Kỳ 30/09/2026, sau freshness gates: FCF score có 62 mã, SBC score có 81 mã, giao của
chúng là Value 52 mã. 49 mã thiếu Value chia thành: 29 chỉ thiếu FCF, 10 chỉ thiếu SBC,
10 thiếu cả hai.

Những lý do của FCF là các nhóm ưu tiên loại trừ nhau trong audit (không phải mọi
nguyên nhân đồng thời của một mã):

| Lý do FCF chưa có score | Số mã | Ví dụ / xử lý |
| --- | ---: | --- |
| Chỉ có broader productive-assets concept cùng kỳ | 15 | FTNT, AMZN, NVDA, ...; concept bao gồm software/intangibles, không alias thành PPE |
| Không có PPE Capex cùng kỳ | 6 | ABNB, ADP, APP, FANG, PDD, ROP |
| Không có annual OCF phù hợp | 4 | ASML, CCEP, FER, TRI; scope/taxonomy còn thiếu |
| Không có instant shares phù hợp | 4 | DASH, META, MSTR, TEAM; không thay bằng weighted-average EPS shares |
| Instant shares gần nhất quá tuổi | 5 | CHTR, CMCSA, DDOG, SHOP, WDAY; không dùng vào normalization |
| Thiếu giá tại signal date | 2 | EA, WBD |
| Nhiều ticker cùng issuer | 2 | GOOG/GOOGL; chưa có market cap theo từng class |
| Chỉ có OCF continuing operations | 1 | GEHC; không alias sang toàn entity |

SBC thiếu ở 20 mã: 8 có allocated-SBC concept, 4 chỉ có revenue bao gồm assessed tax,
5 thiếu annual revenue phù hợp trong freshness window, 3 thiếu reported SBC cùng kỳ. Các concepts này được
giữ trong raw để giải thích gap, không cộng/gộp hay đổi tên thành metric khác.
Allocated SBC expense, gross revenue và PPE/non-PPE capital spending cần construction
riêng/version mới nếu muốn mở rộng factor. Một concept thiếu không đồng nghĩa chi phí bằng 0.

## Policy mẫu nghiên cứu v1

Mỗi ticker–rebalance phải:

1. Có đủ 4 scores và giữ nguyên index grid/provenance.
2. Có CIK, sector, giá nominal dương tại signal date.
3. Tuổi annual flows đang dùng cho FCF/SBC/Growth không quá **550 ngày** tính từ period end.
4. Tuổi instant shares không quá **400 ngày** tính từ measurement date.
5. Tuổi quý Net Income hiện tại không quá **200 ngày** tính từ quarter end.
6. Mọi input đã available theo rule filing day + 1 ngày; không dùng fact mới để sửa ngược quá khứ.

Các giới hạn tuổi là policy baseline bảo thủ, cấu hình qua dbt vars
`max_annual_age_days`, `max_shares_age_days`, `max_quarter_age_days` và lưu trong Gold
coverage manifest. Audit tự đọc policy của Gold build; CLI age flags chỉ lọc mẫu thêm,
không khôi phục các raw inputs đã bị loại khỏi construction. Đổi construction phải
rebuild Intermediate/Gold. Chúng không phải chuẩn kế toán hay thresholds tối ưu performance.
Không lọc trước bằng việc mã có đủ return trong tương lai hay không. Execution lag,
delisting return/exit và chi phí phải được kiểm tra trong backtest engine riêng.

6 mã bị loại khỏi nhóm 54 composite ở bước nạp bổ sung, trước khi áp freshness:

| Mã | Input quá tuổi, số ngày |
| --- | --- |
| CHTR | Shares: 3.744 |
| CMCSA | Shares: 6.117 |
| CPRT | SBC/Growth annual period: 2.983 |
| CTAS | Current NI quarter: 214 |
| DDOG | Shares: 2.830 |
| WDAY | Shares: 2.861 |

Sector selection vẫn lệch: Utilities, Energy và Basic Materials chưa có composite
trong kỳ mới nhất. Chỉ báo cáo trên mẫu đủ điều kiện với benchmark cùng mẫu; không
gọi kết quả đó là backtest đầy đủ NASDAQ-100 hay suy luận alpha từ coverage.

## Cách tái lập và artifacts

```powershell
python -m ingestion.universe_history --resolve-identifiers
python -m ingestion.quality_backfill
# --refresh: bỏ cache SEC; --prices: thử lại các price gaps đã biết

# Từ thư mục dbt, khi không có writer Airflow/IDE:
dbt build --select +gold_compose_score --vars '{allow_observed_universe: true}'

# Từ root:
python -m research.backtest_quality
python -m unittest discover -s tests -v
```

Audit là task `audit_backtest_inputs` sau quality gates trong DAG transformation.
CSV/JSON nằm ở `data/reports/backtest_quality/` (Git ignore): ticker diagnostics,
coverage theo kỳ/ngành, price gaps và assessment/policy. Script chỉ đọc warehouse;
empty research sample hoặc duplicate keys sẽ fail. Dữ liệu raw SEC đầy đủ và manifest
backfill được cache trong `data/raw/`; không commit lên repo.

Notebook `notebooks/backtest_data_quality.ipynb` chạy lại audit và đọc evidence.
`research/evidence/coverage_before_after_2026-10-15.csv` lưu snapshot aggregate đã kiểm
tra của lượt remediation này. Baseline database gốc chỉ lưu local trong `.tmp/`.

## Kiểm chứng sau remediation

- 72 dbt tests pass; warning input coverage còn 127 ticker–kỳ, đều có gap giá.
- 33 Python fixture/ingestion tests pass; 7 Airflow contracts pass trong container.
- Regression test chứng minh stale shares không ảnh hưởng z-score của các issuer khác.
- Test giữ prior-year comparison hợp lệ, dù comparative period vốn cũ hơn freshness window của current period.
- Airflow transformation run `manual__data_quality_20261015T123149`: cả 6 tasks success,
  bao gồm `audit_backtest_inputs` sau Gold và quality gates.
- Toàn bộ 3.939 keys được giữ, không duplicate; input-age policy được ghi trong Gold manifest.

## Giới hạn còn phải giải quyết trước kết luận backtest

- Cần nguồn lịch sử giá/delisting đáng tin cho 6 ticker có gap; không stitch successor tickers.
- Cần effective-date universe và historical sector để bảo vệ kết luận đầy đủ về index.
- Cần construction riêng nếu hỗ trợ IFRS/currency, broader Capex, gross revenue, allocated SBC hoặc class-level market cap.
- Policy sample chỉ kiểm tra input của signal; chưa xác minh thực thi/returns/turnover/costs.

Nguồn chuẩn: [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
SEC companyfacts/submissions được cache và các URL trong registry. SQL kiểm tra nằm
ở `research/sql/backtest_inputs.sql`; test financial semantics chạy SQL production.
