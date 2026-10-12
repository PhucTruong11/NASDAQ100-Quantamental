# Luồng dữ liệu (Data Flow) — Từ thô đến tinh

## 1. Luồng chi tiết từng bước (khớp code thật, cập nhật 2026-10-11)

```mermaid
graph TD
    subgraph SRC[Nguồn dữ liệu]
        A1[SEC EDGAR API - companyfacts]
        A2[Yahoo Finance - giá]
        A3[Yahoo - sector/industry]
        A4[NASDAQ-100 constituent list]
    end

    subgraph ING[Ingestion - Phase 1, song song an toàn]
        B1[edgar.py]
        B2[yahoo.py]
        B3[sector.py]
        B4[universe.py]
    end

    subgraph RAW[Raw landing zone - Parquet]
        C1[(raw/edgar)]
        C2[(raw/prices)]
        C3[(raw/sector)]
        C4[(raw/universe)]
    end

    subgraph BRONZE[dbt Bronze - chỉ typing]
        D1[bronze_company_facts]
        D2[bronze_price_eod]
        D3[bronze_sector]
        D4[bronze_index_membership]
    end

    subgraph STG[dbt Staging - rename, lọc rác]
        E1[stg_fundamentals]
        E2[stg_price_eod]
        E3[stg_sector]
    end

    subgraph SILVER[dbt Silver - dedup, SCD2, PIT]
        F1[silver_fundamentals_pit]
        F2[silver_price_adjusted]
        F3["silver_universe_membership (snapshot SCD2)"]
    end

    subgraph INTER[dbt Intermediate - PLAN, chưa code]
        G0[int_rebalance_universe - xương sống]
        G1[int_momentum_ratio]
        G2[int_growth_ratio]
        G3[int_fcf_yield]
        G4[int_sbc_ratio]
        G5[int_earnings_surprise - SUE]
    end

    subgraph GOLD[dbt Gold - PLAN, chưa code]
        H1[gold_momentum_pillar]
        H2[gold_growth_pillar]
        H3[gold_value_pillar]
        H4[gold_quality_pillar]
        H5[gold_composite_score]
    end

    subgraph OBT[One Big Table - chưa triển khai]
        I1[obt_web]
        I2[obt_nq_contribution]
        I3[obt_nq_breadth]
    end

    subgraph DOWN[Downstream - chưa triển khai]
        J1[Backtest module]
        J2[Streamlit dashboard]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> B4
    B1 --> C1
    B2 --> C2
    B3 --> C3
    B4 --> C4

    C1 --> D1
    C2 --> D2
    C3 --> D3
    C4 --> D4

    D1 --> E1
    D2 --> E2
    D3 --> E3
    D4 --> F3

    E1 --> F1
    E2 --> F2

    F2 --> G0
    F3 --> G0
    E3 --> G0
    G0 --> G1
    G0 --> G2
    G0 --> G3
    G0 --> G4
    G0 --> G5
    F1 -. as-of join .-> G2
    F1 -. as-of join .-> G3
    F1 -. as-of join .-> G4
    F1 -. as-of join .-> G5

    G1 --> H1
    G2 --> H2
    G3 --> H3
    G4 --> H3
    G5 --> H4
    H1 --> H5
    H2 --> H5
    H3 --> H5
    H4 --> H5

    H5 --> I1
    F2 --> I2
    F2 --> I3

    I1 --> J1
    I1 --> J2
    I2 --> J2
    I3 --> J2
```

**Bronze/Staging/Silver: đã code xong, parse sạch.** **Intermediate/Gold: đang ở giai đoạn plan**, tên model trong sơ đồ là tên dự kiến theo `intermediate-layer-plan.md`, chưa có file `.sql` thật.

## 2. Giải thích từng giai đoạn

| Giai đoạn | Input | Output | Việc chính |
| --- | --- | --- | --- |
| Ingestion (E+L) | EDGAR, Yahoo (giá + sector), danh sách NASDAQ-100 | Parquet raw (4 nguồn) | Fetch thô, validate schema (Pydantic), retry (tenacity), watermark theo file JSON per-ticker để không ingest trùng |
| Bronze | Parquet raw | `bronze_*` (4 bảng) | Load vào DuckDB qua `source()`, chỉ ép kiểu (`safe_cast`/`clean_string`), không sửa logic |
| Staging | `bronze_*` | `stg_*` (3 bảng — sector dừng ở đây) | Đổi tên cột sang snake_case, lọc rác rõ ràng (không phải quyết định nghiệp vụ) |
| Silver | `stg_*` | `silver_*` (3 bảng) | Dedup theo `(cik, metric_tag, unit, period_start, period_end)` giữ bản sớm nhất; tính `adjusted_close_price` chỉ từ cổ tức (không nhân split — Yahoo đã điều chỉnh sẵn); SCD2 cho universe qua `dbt snapshot` |
| Intermediate | `silver_*` | `int_*` | `int_rebalance_universe` làm xương sống (ticker × rebalance_date, chuẩn hoá về snake_case `ticker`/`cik`), các `int_*` còn lại as-of join vào đó, tính ratio rồi z-score sector-neutral (`n>=5`, fallback toàn universe) |
| Gold | `int_*` (đã z-score) | `gold_*` | Gộp 4 pillar, `gold_composite_score = 0.25×mỗi pillar` (equal-weight), NULL nếu thiếu pillar (vd 6 mã FPI không có fundamentals) |
| OBT | `gold_*`, `silver_*` | `obt_*` | **Chưa triển khai** — một bảng phẳng cho dashboard + bảng riêng phân tích NQ, tên dự kiến |
| Downstream | `obt_*` | Kết quả backtest, dashboard | **Chưa triển khai** — Backtest tính IC/turnover/quintile return; Streamlit đọc trực tiếp |

## 3. Airflow điều phối theo giai đoạn nào

| DAG | Giai đoạn phụ trách |
| --- | --- |
| `universe_membership_dag` | Ingestion → Bronze → Silver cho `silver_universe_membership` |
| `price_eod_dag` | Ingestion → Bronze → Silver cho `silver_price_adjusted` |
| `fundamentals_dag` | Ingestion → Bronze → Silver cho `silver_fundamentals_pit` |
| `sector_dag` | Ingestion → Bronze → Staging cho `stg_sector` (không cần lịch dày, sector ít đổi) |
| `dbt_transform_dag` | Staging → Intermediate → Gold, **1 task duy nhất gọi `dbt build`** (đã chốt dùng BashOperator, không dùng Cosmos task-per-model — chi tiết và lý do ở `execution-flow.md`) |
| `backtest_dag` | OBT → kết quả backtest |

Thứ tự thô → tinh luôn đi một chiều: **Raw Parquet → Bronze → Staging → Silver → Intermediate → Gold → OBT**. Không có bước nào ghi ngược lại tầng trước, giúp dễ debug: lỗi ở đâu chỉ cần chạy lại từ đúng tầng đó.

## 4. Data flow phân rã theo từng phần phức tạp

### 4.1 Ingestion & Idempotency

```mermaid
graph TD
    A[Start ingestion job] --> B{Check watermark<br/>file JSON per-ticker, đã cập nhật hôm nay chưa?}
    B -- Chưa / cũ --> C[Gọi API nguồn<br/>EDGAR hoặc Yahoo]
    B -- Đã cập nhật --> Z[Skip, không gọi API]
    C --> D{Rate limit OK?<br/>EDGAR 10 req/s}
    D -- Vượt limit --> E[tenacity retry + backoff]
    E --> C
    D -- OK --> F[Validate schema Pydantic]
    F -- Lỗi schema --> G[Log lỗi, không ghi raw]
    F -- Hợp lệ --> H[Ghi Parquet raw]
    H --> I{run() trả về True cho CẢ 2<br/>edgar + yahoo?}
    I -- Có --> J[Set watermark]
    I -- Không --> K[Không set — retry lại ở lần chạy sau]
```

*Chạy lại job này nhiều lần trong ngày không tạo dữ liệu trùng. `run()` chỉ trả `True` khi ghi thành công thật, tránh watermark bị set sai khi fetch lỗi ngầm.*

### 4.2 Point-in-time fundamentals join

```mermaid
graph TD
    A[silver_fundamentals_pit] --> B["Dedup theo (cik, metric_tag, unit_of_measure, period_start_date, period_end_date)<br/>giữ filing_date SỚM NHẤT, tie-break ưu tiên 10-K (được kiểm toán)"]
    B --> C{6 CIK không có 10-K/10-Q?<br/>ASML, ARM, PDD, CCEP, TRI, FER}
    C -- Có --> D[Không có dòng nào trong bảng<br/>Intermediate: NULL cho các ratio cần fundamentals]
    C -- Không --> E[int_rebalance_universe as-of join<br/>chỉ dùng fundamentals có available_date <= rebalance_date]
    D --> F["gold_composite_score = NULL — loại khỏi backtest/screener,<br/>vẫn giữ trong universe, Momentum pillar vẫn tính được"]
    E --> G[Sẵn sàng cho backtest, không look-ahead]
```

### 4.3 Universe membership (SCD2) & truy vấn point-in-time

```mermaid
graph TD
    A[Danh sách NASDAQ-100 hiện tại] --> B[dbt snapshot<br/>silver_universe_membership SCD2, strategy=check]
    B --> C["(dbt_valid_from, dbt_valid_to), invalidate_hard_deletes=True"]
    C --> D{int_rebalance_universe cho rebalance_date t}
    D --> E["Query: WHERE dbt_valid_from <= t AND (dbt_valid_to > t OR dbt_valid_to IS NULL)"]
    E --> F[Universe đúng tại thời điểm t]
    F --> G["⚠ Snapshot mới chạy từ hiện tại — mọi dbt_valid_from đều là ngày bắt đầu chạy,<br/>backtest quá khứ vẫn dùng universe HIỆN TẠI (survivorship bias đã ghi nhận)"]
```

### 4.4 Factor → Composite score

```mermaid
graph TD
    A[int_* ratio factors] --> B[Winsorize outlier từng factor]
    B --> C{Sector có n >= 5 mã<br/>không NULL cho factor đó? count(column), không phải count(*)}
    C -- Có --> D[Z-score trong sector]
    C -- Không --> E[Z-score toàn universe - fallback<br/>áp dụng cho Utilities 4, Energy 2, 3 sector còn lại 1 mã]
    D --> F{Check tương quan chéo<br/>giữa factor - VIF}
    E --> F
    F -- Cao --> G[Orthogonalize / loại bớt factor trùng]
    F -- OK --> H[Gộp thành pillar<br/>Growth / Value / Momentum / Quality]
    G --> H
    H --> I["gold_composite_score = 0.25×mỗi pillar<br/>(equal-weight, đã chốt)"]
    I --> J{Thiếu pillar nào?<br/>vd 6 mã FPI không có 10-K/10-Q}
    J -- Thiếu --> K["composite_score = NULL<br/>KHÔNG coalesce về 0, loại khỏi backtest/screener"]
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

*Chưa triển khai — giữ nguyên plan từ `analytics-flow.md`.*

### 4.6 Airflow DAG dependency & DuckDB concurrency

Chi tiết đầy đủ (nguyên lý tách 2 phase, lý do bỏ Cosmos, cơ chế Pool) đã chuyển hẳn sang `execution-flow.md` — không lặp lại ở đây để tránh 2 nguồn thông tin lệch nhau theo thời gian.