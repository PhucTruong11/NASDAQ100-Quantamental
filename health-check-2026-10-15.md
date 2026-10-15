# Health check — 15/10/2026

> Snapshot dưới đây ghi nhận trước đợt sửa DAG/tests/coverage. Đợt triển khai sau đã nối Gold, thêm 7 tests, truyền research mode và chạy thành công cả hai DAG; xem [Gold runtime guide](gold-runtime-guide.md) để biết trạng thái hiện tại và bằng chứng kiểm chứng mới.

**Kết luận:** Gold build và các kiểm tra dữ liệu bên dưới pass; Postgres/webserver/scheduler đang hoạt động. Pipeline tự động chưa hoàn chỉnh đến Gold, strict universe gate vẫn chặn nguồn observation như thiết kế, và runtime scheduler còn xung đột dependencies.

## Kết quả Gold và dbt local

Đã chạy trên warehouse thật `data/warehouse.duckdb` sau khi sửa các lỗi tên cột/cú pháp trong Gold/schema. Lần build local dùng landing zone có sẵn. Trong lúc kiểm tra hạ tầng, một run ingestion Airflow cũng đã hoàn tất tải current universe và sector; không tải lại API giá/EDGAR.

| Kiểm tra | Kết quả |
| --- | --- |
| `python -m unittest discover -s tests -v` | 13 tests pass |
| `dbt build --select +gold_compose_score --vars '{allow_observed_universe: true}' --quiet` từ thư mục dbt | Exit 0; 24 node success, 65 data tests pass, 1 warning |
| 7 Gold semantic audits từ mục 13 của roadmap | 0 vi phạm ở cả 7 queries |
| Số dòng của mỗi Gold model | 3.939, bằng grid |
| Schema Gold | Composite 13 cột; Growth/Momentum/Quality 5 cột; Value 6 cột |
| Giá gần nhất trong Silver | 14/10/2026 |
| Kỳ scoring hoàn tất gần nhất | 30/09/2026 |
| Membership mode | Toàn bộ 3.939 dòng hiện là monthly_observation |

Các semantic audits được render và chạy chỉ đọc trên warehouse; chưa tự động thêm thành các file `dbt/tests/assert_gold_*.sql`. Các tests dbt trong schema Gold hiện chủ yếu not-null keys/metadata.

Cả năm bảng đều có **0 khóa trùng** `(ticker, rebalance_date)` trong lần kiểm tra này. Nên đưa uniqueness và 7 semantic audits vào test suite dbt để các lần build sau cũng tự kiểm tra được; kiểm tra thủ công hôm nay không thay thế các regression tests đó.

## Các lỗi gõ đã sửa

- Composite: thêm `THEN` trong bốn CASE đếm pillar.
- Momentum: `momentu_score` → `momentum_score`.
- Quality: join `m.rebalance = u.rebalance` → `m.rebalance_date = u.rebalance_date`.
- Value: `q.rabalance_date` → `q.rebalance_date`; `sbc_zcore` → `sbc_zscore`.
- Gold schema: ba cột `rebalance` → `rebalance_date` ở Growth, Value, Composite.

Giữ nguyên cách bạn tính và đặt trọng số các factor/pillars.

## Coverage còn thiếu

Warning `assert_universe_input_coverage` trả **465 ticker–kỳ**, không phải 465 ticker khác nhau. Các nhóm sau có thể chồng nhau:

| Loại thiếu | Số ticker–kỳ |
| --- | --- |
| CIK | 426 |
| Giá đúng rebalance_date | 127 |
| Sector | 49 |

Ở kỳ 30/09/2026:

| Chỉ tiêu | Có score / tổng universe |
| --- | --- |
| Growth | 86 / 101 |
| Momentum | 99 / 101 |
| Value | 51 / 101 |
| Quality proxy | 93 / 101 |
| Composite / eligible | 49 / 101 |

Đây là kết quả coverage, chưa phải kết quả backtest. Strict mode vẫn yêu cầu effective-date history; việc build pass với observation mode không thay đổi chất lượng nguồn.

## Hạ tầng

Docker Desktop ban đầu chưa chạy; đã khởi động và chạy `docker compose up -d`. Đã bỏ cài thêm data packages ở webserver/init bằng environment override trong Compose; scheduler vẫn cài dependencies phục vụ jobs. Init exit 0.

| Kiểm tra | Kết quả |
| --- | --- |
| Postgres `pg_isready`, `airflow db check` | Pass |
| `http://localhost:8080/health` | HTTP 200; metadatabase healthy; scheduler healthy |
| Scheduler heartbeat | `airflow jobs check --job-type SchedulerJob --local`: Found one alive job |
| DAG import errors sau khi cài dependencies | `[]` |
| `airflow dags test test_duckdb_connect 2026-10-15` | DAG/task success; đọc được 6 bảng Intermediate, mỗi bảng 3.939 dòng |
| Bộ 13 tests semantics chạy lại trong scheduler container | 13 pass |
| DuckDB 1.1.0 trong container đọc Gold | 3.939 dòng composite; không lỗi tương thích file |
| `dbt debug` trong container | Connection OK; command exit 1 do thiếu Git executable |
| `pip check` trong scheduler | Exit 1: xung đột protobuf/cffi/opentelemetry với providers trong image Airflow |

Tại thời điểm kiểm tra, runtime scheduler là Airflow 2.9.3 / DuckDB 1.1.0 / dbt-core 1.12.6 / dbt-duckdb 1.11.0 / Polars 2.0.0 / yfinance 1.7.0. Local đang dùng DuckDB 1.5.5, dbt-core 1.12.5, Polars 1.44.2. Vì dependencies chưa khóa đầy đủ nên hai môi trường khác nhau.

Một lần `dbt debug` ban đầu gặp lock do transformation DAG đang chạy; sau khi DAG hoàn tất, connection check đã pass. Không chạy thêm dbt writer đồng thời với DAG ghi cùng warehouse.

Core services đang hoạt động nhưng môi trường dependencies chưa sạch; chưa đánh dấu toàn hệ thống production-ready. UI: `http://localhost:8080` (tài khoản local theo Compose).

## Kết quả DAG thực tế

- `dbt_transformation_pipeline`, run `manual__2026-10-15T07:06:15+00:00`: 4 task success với `allow_observed_universe=true`.
- `nasdaq100_ingestion_pipeline`, run `manual__2026-10-15T07:06:05.664733+00:00`: success; Yahoo sector lấy đủ 100/100 ticker.
- DAG ingestion dùng `wait_for_completion=False`, nên ingestion success chỉ chứng minh đã trigger DAG con. Nó chưa truyền research-mode flag sang transformation: DAG con mặc định `allow_observed_universe=false`, không chấp nhận monthly observations.
- Run con `manual__2026-10-15T07:13:05.891852+00:00` đã bị strict gate `assert_rebalance_universe_semantics` chặn ở Intermediate. Đây là guard về chất lượng nguồn; không tắt guard để biến lịch sử quan sát thành lịch sử effective-date.
- Run strict đã kết thúc failed sau retry; không còn task active. Sau đó đã rebuild local toàn chuỗi `+gold_compose_score` với observation mode để Intermediate/Gold nhất quán trên landing zone mới. Kết quả cuối vẫn 24 node success, 65 tests pass, 1 warning; chạy lại cả 7 Gold semantic audits đều 0 vi phạm. Health endpoint lúc 14:17 giờ Việt Nam vẫn báo metadatabase/scheduler healthy.

## Phạm vi còn phải phân biệt

- DAG transformation hiện chưa có task build Gold; build Gold thành công ở trên là lần chạy dbt local.
- Ingestion DAG hiện chỉ tải current universe/sector, chưa tự tải giá/EDGAR/benchmark.
- Các kiểm tra hạ tầng không tự chứng minh một full ingestion → Gold → backtest pipeline đã chạy.
- Không commit/push trong lần health check này.

## Thứ tự xử lý tiếp

1. Bổ sung unique grain và semantic tests Gold vào dbt, dựa trên mục 13 của `post-gold-roadmap.md`.
2. Thêm task build Gold vào transformation DAG trước quality gates; truyền research-mode parameter có chủ đích từ ingestion sang DAG con. Với dữ liệu hiện tại, bật observation mode khi chạy nghiên cứu; strict mode vẫn phải chặn nguồn monthly observations. Dùng một Airflow pool giới hạn một DuckDB writer cho mọi DAG ghi cùng file, đồng thời tránh chạy dbt local khi DAG đang ghi.
3. Điều tra missing CIK/sector/price và lý do Value chỉ có 51/101 scores ở kỳ gần nhất; giữ NULL đúng nghĩa, không tự điền 0 để tăng coverage.
4. Khóa dependencies và tách runtime dbt khỏi các providers Airflow bị xung đột; thêm Git vào image nếu cần đạt đủ kiểm tra `dbt debug`. Chuyển cài packages từ lúc container khởi động sang bước build image để startup ổn định hơn.
5. Sau đó mới nối backtest và hiển thị rõ hạn chế lịch sử universe khi báo cáo kết quả.
