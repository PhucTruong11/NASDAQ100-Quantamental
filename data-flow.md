# Luồng dữ liệu (Data Flow) — Từ thô đến tinh

## 1. Luồng chi tiết từng bước

```mermaid
graph TD
    subgraph SRC[Nguồn dữ liệu]
        A1[SEC EDGAR API<br/>companyfacts - filing_date lấy từ field filed trong response]
        A2[Price API<br/>yfinance / Finnhub]
    end

    subgraph ING[Ingestion - Extract and Load]
        B1[Python + Polars + Pydantic<br/>validate schema, retry/backoff]
        B2[Watermark check<br/>ingestion_watermark]
    end

    subgraph RAW[Raw landing zone - Parquet]
        C1[(raw/price_eod)]
        C2[(raw/company_facts)]
        C3[(raw/index_membership)]
    end

    subgraph BRONZE[dbt Bronze - chỉ typing]
        D1[bronze_price_eod]
        D2[bronze_company_facts]
        D4[bronze_index_membership]
    end

    subgraph STG[dbt Staging - rename, loc du lieu rac]
        E1[stg_price_eod]
        E2[stg_fundamentals]
    end

    subgraph SILVER[dbt Silver - dedupe, SCD2, PIT]
        F1[silver_universe_membership<br/>snapshot SCD2]
        F2[silver_price_adjusted<br/>áp dụng split/dividend]
        F3[silver_fundamentals_pit<br/>join theo filing_date]
    end

    subgraph INTER[dbt Intermediate - ratio, lag, zscore]
        G1[int_momentum_ratio]
        G2[int_growth_ratio]
        G3[int_fcf_yield]
        G4[int_sbc_ratio]
        G5[int_earnings_surprise]
        G6[macro zscore_sector_neutral]
    end

    subgraph GOLD[dbt Gold - trụ cột factor]
        H1[gold_momentum_pillar]
        H2[gold_growth_pillar]
        H3[gold_value_pillar]
        H4[gold_quality_pillar]
        H5[gold_composite_score]
    end

    subgraph OBT[One Big Table]
        I1[obt_web]
        I2[obt_nq_contribution]
        I3[obt_nq_breadth]
    end

    subgraph DOWN[Downstream]
        J1[Backtest module<br/>IC, quintile return, turnover]
        J2[Streamlit dashboard]
    end

    A1 --> B1
    A2 --> B1
    B1 --> B2
    B2 --> C1
    B2 --> C2
    B2 --> C3

    C1 --> D1
    C2 --> D2
    C3 --> D4

    D1 --> E1
    D2 --> E2
    D4 --> F1

    E1 --> F2
    E2 --> F3

    F1 --> G1
    F1 --> G2
    F2 --> G1
    F3 --> G2
    F3 --> G3
    F3 --> G4
    F3 --> G5

    G1 --> G6
    G2 --> G6
    G3 --> G6
    G4 --> G6
    G5 --> G6

    G6 --> H1
    G6 --> H2
    G6 --> H3
    G6 --> H4
    H1 --> H5
    H2 --> H5
    H3 --> H5
    H4 --> H5

    H5 --> I1
    F1 --> I2
    F2 --> I2
    F2 --> I3

    I1 --> J1
    I1 --> J2
    I2 --> J2
    I3 --> J2
```

## 2. Giải thích từng giai đoạn

| Giai đoạn | Input | Output | Việc chính |
| --- | --- | --- | --- |
| Ingestion (E+L) | EDGAR, price API | Parquet raw | Fetch thô, validate schema (Pydantic), kiểm watermark để không ingest trùng |
| Bronze | Parquet raw | `bronze_*` | Load vào DuckDB, chỉ ép kiểu dữ liệu, không sửa logic |
| Staging | `bronze_*` | `stg_*` | Đổi tên cột, lọc field cần dùng, chuẩn hoá định dạng |
| Silver | `stg_*` | `silver_*` | Dedupe, SCD2 cho universe membership, điều chỉnh giá theo corporate actions, join fundamentals theo **filing date** (point-in-time) |
| Intermediate | `silver_*` | `int_*` | Tính ratio từng factor (momentum, growth, FCF yield, SBC ratio, earnings surprise), rồi chuẩn hoá z-score trung hoà theo sector |
| Gold | `int_*` (đã z-score) | `gold_*` | Gộp thành trụ cột (pillar) và điểm tổng hợp `gold_composite_score` |
| OBT | `gold_*`, `silver_*` | `obt_*` | Một bảng phẳng cho dashboard, cộng thêm bảng riêng cho phân tích NQ (contribution, breadth) |
| Downstream | `obt_*` | Kết quả backtest, dashboard | Backtest tính IC/turnover/quintile return; Streamlit đọc trực tiếp để hiển thị |

## 3. Airflow điều phối theo giai đoạn nào

| DAG | Giai đoạn phụ trách |
| --- | --- |
| `universe_membership_dag` | Ingestion → Bronze → Silver cho `silver_universe_membership` |
| `price_eod_dag` | Ingestion → Bronze → Silver cho `silver_price_adjusted` |
| `fundamentals_dag` | Ingestion → Bronze → Silver cho `silver_fundamentals_pit` |
| `dbt_transform_dag` (Cosmos) | Staging → Intermediate → Gold → OBT, chạy sau khi 3 DAG trên hoàn tất |
| `backtest_dag` | OBT → kết quả backtest |

Thứ tự thô → tinh luôn đi một chiều: **Raw Parquet → Bronze → Staging → Silver → Intermediate → Gold → OBT**. Không có bước nào ghi ngược lại tầng trước, giúp dễ debug: lỗi ở đâu chỉ cần chạy lại từ đúng tầng đó.

## 4. Data flow phân rã theo từng phần phức tạp

### 4.1 Ingestion & Idempotency

```mermaid
graph TD
    A[Start ingestion job] --> B{Check ingestion_watermark<br/>đã có mốc mới nhất chưa?}
    B -- Chưa có / cũ --> C[Gọi API nguồn<br/>EDGAR hoặc Price API]
    B -- Đã đủ mới nhất --> Z[Skip, không gọi API]
    C --> D{Rate limit OK?<br/>EDGAR 10 req/s, Finnhub 60/phút}
    D -- Vượt limit --> E[Backoff + retry]
    E --> C
    D -- OK --> F[Validate schema Pydantic]
    F -- Lỗi schema --> G[Log lỗi, không ghi raw]
    F -- Hợp lệ --> H[Ghi Parquet raw]
    H --> I[Update ingestion_watermark]
    I --> J[Kết thúc - idempotent nếu chạy lại]
```

*Chạy lại job này nhiều lần trong ngày không tạo dữ liệu trùng, nhờ watermark chặn ngay từ đầu.*

### 4.2 Point-in-time fundamentals join

```mermaid
graph TD
    A[silver_fundamentals_pit] --> B["Dedup theo (cik, metric_tag, period_start, period_end, unit_of_measure)<br/>giữ filing_date SỚM NHẤT"]
    B --> C{6 CIK không có 10-K/10-Q?<br/>ASML, ARM, PDD, CCEP, TRI, Ferrovial}
    C -- Có --> D[Không có dòng nào trong bảng<br/>Gold: NULL cho Value/Growth/Quality pillar]
    C -- Không --> E[Join với giá tại thời điểm t<br/>chỉ dùng fundamentals có filing_date <= t]
    D --> F["composite_score = NULL<br/>loại khỏi backtest/screener, vẫn giữ trong universe"]
    E --> G[Sẵn sàng cho backtest, không look-ahead]
```

### 4.3 Universe membership (SCD2) & truy vấn point-in-time

```mermaid
graph TD
    A[Danh sách NASDAQ-100 hàng tháng] --> B[dbt snapshot<br/>silver_universe_membership SCD2]
    B --> C[(valid_from, valid_to, is_current)]
    C --> D{Backtest cho ngày t}
    D --> E["Query: WHERE valid_from <= t AND valid_to > t"]
    E --> F[Universe đúng tại thời điểm t<br/>gồm cả mã đã bị loại sau này]
    F --> G[Tránh survivorship/look-ahead bias]
```

### 4.4 Factor → Composite score

```mermaid
graph TD
    A[int_* ratio factors] --> B[Winsorize outlier từng factor]
    B --> C{Sector có n >= 5 mã<br/>không NULL cho factor đó?}
    C -- Có --> D[Z-score trong sector]
    C -- Không --> E[Z-score toàn universe - fallback]
    D --> F{Check tương quan chéo<br/>giữa factor - VIF}
    E --> F
    F -- Cao --> G[Orthogonalize / loại bớt factor trùng]
    F -- OK --> H[Gộp thành pillar<br/>Growth / Value / Momentum / Quality]
    G --> H
    H --> I["composite_score = 0.25×mỗi pillar<br/>(equal-weight, đã chốt)"]
    I --> J{Thiếu pillar nào?<br/>vd 6 mã FPI không có 10-K/10-Q}
    J -- Thiếu --> K[composite_score = NULL<br/>loại khỏi backtest/screener]
    J -- Đủ cả 4 --> L[composite_score hợp lệ]
```

### 4.5 Backtest

```mermaid
graph TD
    A[obt_web: composite score theo ngày] --> B[Chia quintile Q1-Q5 mỗi kỳ]
    B --> C[Tính forward return mỗi quintile]
    C --> D[Tính IC: correlation score vs return kỳ sau]
    D --> E[Tính turnover giữa các kỳ]
    E --> F[Trừ chi phí giao dịch giả định]
    F --> G[So sánh benchmark QQQ]
    G --> H[Kết quả backtest]
```

### 4.6 Airflow DAG dependency & DuckDB concurrency

```mermaid
graph TD
    A[universe_membership_dag] --> D[dbt_transform_dag]
    B[price_eod_dag] --> D
    C[fundamentals_dag] --> D
    D --> E{Task ghi DuckDB?}
    E -- Có --> F[Chạy tuần tự - dependency tường minh]
    E -- Chỉ đọc --> G[Có thể chạy song song]
    F --> H[Đóng connection trong try/finally]
    H --> I[backtest_dag chạy sau khi transform xong]
```