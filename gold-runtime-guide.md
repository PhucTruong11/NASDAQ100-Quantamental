# Gold MVP: build, tests, coverage và research mode

## Flow đã triển khai

`Ingestion universe/sector → trigger transformation và đợi kết quả`

`Bronze → seed/snapshot/Silver → Intermediate → Gold + coverage → quality gates toàn project → backtest input audit`

Audit dữ liệu và policy mẫu hiện tại được chốt trong [backtest-data-quality.md](backtest-data-quality.md).
Task cuối xuất `is_research_signal_eligible` kèm tuổi input; có đủ 4 scores không tự đảm bảo input còn phù hợp.

Transformation build đủ 5 bảng: Growth, Momentum, Value, Quality proxy, Composite.
Gold giữ grain `(ticker, rebalance_date)`, giữ đủ grid, giữ NULL đúng nghĩa.

Tất cả tasks transformation và DAG đọc thử DuckDB dùng pool `duckdb_access` có 1 slot.
Compose init tạo pool; với stack đang chạy có thể tạo/cập nhật bằng:

```powershell
docker compose exec -T airflow-scheduler airflow pools set duckdb_access 1 "Serialize DuckDB warehouse access"
```

Mỗi DAG chính giới hạn `max_active_runs=1`. Pool bảo vệ truy cập warehouse giữa
tasks Airflow; lệnh dbt local/IDE không tham gia pool. Không chạy local writer cùng
lúc DAG ghi warehouse. Các task không tạo snapshot bất biến của toàn landing zone.

Compose ghim DuckDB 1.5.5 cùng local vì bản 1.1.0 đã phát sinh lỗi đọc metadata sau
khi warehouse được rebuild bằng bản mới. Các direct dependencies của scheduler cũng
được ghim theo runtime đã kiểm tra; pip cache lưu dưới `data/.cache/pip` (Git ignore).
Đây chưa là transitive lockfile hay image build hoàn chỉnh. Các xung đột provider
protobuf/cffi/opentelemetry của image Airflow cũ vẫn cần xử lý ở bước tách runtime.

## Chạy nghiên cứu

Cả hai DAG dùng Param Boolean `allow_observed_universe`, mặc định **false**.
Trong form Trigger DAG của Airflow, chọn true khi chủ đích dùng monthly/live
observations cho nghiên cứu. DAG ingestion truyền Boolean này qua `dag_run.conf`
sang transformation; các lệnh dbt nhận cùng JSON qua environment variable.
Không truyền chuỗi `"true"`/`"false"` thay Boolean.

```powershell
# Trigger ingestion và tự nối đến Gold + quality gates
docker compose exec -T airflow-scheduler airflow dags trigger nasdaq100_ingestion_pipeline --conf '{"allow_observed_universe": true}'

# Chỉ transformation, dùng raw đã có
docker compose exec -T airflow-scheduler airflow dags trigger dbt_transformation_pipeline --conf '{"allow_observed_universe": true}'

# Local: từ thư mục dbt, khi Airflow không ghi warehouse
dbt build --select +gold_compose_score --vars '{allow_observed_universe: true}'

# Chỉ kiểm tra lại Gold; không export report build mới
dbt test --select gold --vars '{allow_observed_universe: true}'
```

Nếu DAG đang paused, unpause trước khi chạy. Không bật research mode mặc định để
làm strict run pass. Strict gate vẫn phải fail nếu không có verified effective history.
Ingestion success giờ đòi hỏi DAG con success; trigger task không giữ DuckDB pool
trong lúc đợi để tránh chặn chính DAG con.

Ingestion hiện chỉ refresh universe/sector, chưa fetch giá, EDGAR hoặc benchmark.
Sector refresh không đầy đủ sẽ fail task thay vì chấp nhận file cũ còn trên đĩa.

## Định nghĩa Quality đã chốt cho MVP

Definition ID: `quarterly_net_income_yoy_growth_proxy_v1`.

`raw = (NI_quý_gần_nhất - NI_quý_cùng_kỳ_năm_trước) / abs(NI_quý_cùng_kỳ_năm_trước)`

Intermediate chọn các fact NetIncomeLoss/USD duration quý và đã available trước
rebalance theo policy hiện có, chuẩn hóa ngành bằng macro hiện tại, rồi clip [-3, 3].
Mẫu số bằng 0 hoặc thiếu input → NULL. Gold passthrough z-score sang `quality_score`.
Điểm cao thể hiện tăng trưởng lợi nhuận quý mạnh hơn theo proxy này.

Giữ tên `int_earnings_surprise` / `earnings_surprise_zscore` để tương thích. Ý nghĩa
hiện tại là earnings-growth proxy, không phải actual-vs-analyst-consensus surprise,
SUE chuẩn hóa theo độ biến động earnings changes, hay quality tổng hợp profitability/stability.
Chưa dựng Q4 từ annual − YTD nếu nguồn không có fact quý riêng. Đổi định nghĩa sau
này cần version mới và chạy lại nghiên cứu; không chỉ đổi nhãn score.

## Tests tự động

7 singular tests `dbt/tests/assert_gold_*.sql` (severity ERROR):

1. Unique grain cho cả 5 bảng.
2. Đúng tập keys của universe, bắt cả mất/thừa keys.
3. Growth/Momentum/Quality bằng score nguồn, kể cả NULL.
4. Value đủ FCF/SBC mới tính 50/50.
5. Composite đếm pillar, eligibility, 25% mỗi pillar và các cột pillar sao chép đúng.
6. Sector cả 5 bảng và CIK/membership/identifier provenance của Composite đúng grid.
7. Score hữu hạn và trong [-3, 3] theo construction hiện tại.

Schema tests giữ not-null keys/metadata. Không áp not-null lên mọi score.
Fixtures Python chạy SQL thật để kiểm tra NULL, zero, nhiều kỳ, duplicate, mất/thừa
keys, sai công thức/provenance và NaN; Airflow contracts chạy trong container.

## Coverage sau mỗi full Gold build

Hook `on-run-end` tự export khi **cả 5 Gold models build success trong cùng command**,
dù chạy CLI hay Airflow. Không cần nhớ chạy script report riêng.

Files dưới `data/reports/gold/`, prefix là dbt invocation ID:

| File | Nội dung |
| --- | --- |
| `*-by_period.csv` | Số mã có từng pillar/composite, tỷ lệ composite, các input thiếu theo kỳ |
| `*-by_sector.csv` | Cùng chỉ tiêu theo kỳ/ngành, giữ bucket sector thiếu để kiểm đếm |
| `*-input_diagnostics.csv` | Chi tiết ticker–kỳ: scores, CIK/price/sector, OCF/Capex/shares/market-cap, SBC/revenue và các flags |
| `*-run.json` | Research flag, membership labels, Quality definition, validation của command và coverage kỳ gần nhất |

dbt log in dòng coverage kỳ gần nhất cùng prefix file. Báo cáo thiếu input có các
nhóm chồng nhau; không cộng chúng thành số mã thiếu duy nhất. Có raw ratio nhưng
không có z-score còn có thể do thiếu số quan sát/dispersion để chuẩn hóa ngành.
Hiện chưa đặt tỷ lệ coverage tối thiểu tùy ý; báo cáo giữ nguyên denominator universe,
warning input coverage hiện có vẫn hoạt động, mọi vi phạm cấu trúc/công thức là ERROR.

Nếu models đã build đủ nhưng test của command fail, report ghi
`selected_build_validation=failed`. Nếu model build lỗi/partial selection, không xuất
report đầy đủ mới từ các bảng cũ. `dbt test` riêng cũng không xuất report build mới.
Validation trong JSON chỉ áp dụng command đó; DAG cuối còn phải qua toàn bộ quality gates.
Giữ thư mục report có sẵn; biến `gold_coverage_dir` chỉ dùng khi thư mục đích đã tồn tại.

Các files dữ liệu report được Git ignore. NULL không được thay bằng 0 để tăng coverage;
`is_score_eligible=true` không chứng minh lịch sử universe chính xác và chưa là kết quả backtest.

## Kiểm chứng 15/10/2026

- Local full build: 72 data tests pass, 1 warning input coverage (465 ticker–kỳ).
- 22 Python semantics/Gold fixture tests pass; 6 Airflow contracts pass trong image Airflow.
- Ingestion run `manual__gold_validation_20261015T081213`: success.
- Child transformation run `manual__2026-10-15T08:13:53.853521+00:00`: success, cả 5 tasks success; conf nhận Boolean research=true.
- Bronze attempt đầu gặp lỗi metadata của DuckDB 1.1.0; sau khi đồng bộ 1.5.5, retry và toàn flow đã success. Compose đã ghim runtime mới.
- Coverage invocation `c42908ba-e434-477f-90d4-757c69ede81c`: đã đối chiếu CSV/JSON với warehouse, đủ 39 kỳ / 459 nhóm kỳ–ngành / 3.939 ticker–kỳ.
- Kỳ 30/09/2026: 49/101 mã đủ composite (48,51%); Value 51/101. Không tự tăng coverage bằng điền 0 hoặc giảm universe.

Lượt này kiểm tra pipeline ingestion universe/sector và transformation từ raw hiện có;
chưa chạy một ingestion đầy đủ giá/EDGAR/benchmark hay performance backtest.
