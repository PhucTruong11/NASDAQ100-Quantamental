# Mẫu Gold để tự đọc và triển khai

Các SQL dưới đây **chỉ là mẫu trong tài liệu**. Chưa viết vào `dbt/models/gold/` và chưa chạy lên warehouse. Bạn dùng chúng để hiểu cấu trúc, rồi tự triển khai từng model.

Mẫu bám theo tên file và columns hiện tại. Gold nhận **z-score đã tính ở Intermediate**, gộp thành pillar rồi composite; không tự lấy lại facts, tính FCF hay chuẩn hóa z-score lần nữa.

## 1. Quy tắc chung trước khi viết

| Điều cần giữ | Vì sao? |
| --- | --- |
| Grain `(ticker, rebalance_date)` | Một ticker có nhiều tháng; join chỉ ticker sẽ nhân dòng/trộn kỳ |
| Dùng `int_rebalance_universe` làm bảng gốc | Giữ toàn bộ universe, kể cả các mã thiếu score |
| `LEFT JOIN` factors/pillars | Để thiếu dữ liệu thể hiện bằng NULL thay vì biến mất |
| Lấy sector/provenance từ bảng gốc | Tránh chọn nhiều cột sector hoặc mất nhãn observation |
| Liệt kê output columns rõ ràng | Không kéo toàn bộ audit columns của Intermediate qua Gold bằng SELECT * |
| Không thay NULL score bằng 0 | Điểm 0 có thể là tín hiệu trung bình thật; NULL là chưa biết |
| Kiểm tra upstream unique trước join | LEFT JOIN vẫn nhân dòng nếu factor/pillar có duplicate keys |

**Đủ 5 mẫu, theo tên file hiện tại:**

| Mẫu | File bạn tự viết | Input chính | Công việc |
| --- | --- | --- | --- |
| A — Growth | `gold_growth_pillar.sql` | growth_zscore | Alias score một factor |
| B — Momentum | `gold_momentum_pillar.sql` | momentum_zscore | Alias score một factor |
| C — Value | `gold_value_pillar.sql` | fcf_yield_zscore + sbc_zscore | Gộp 0.5 mỗi factor |
| D — Quality proxy | `gold_quality_pillar.sql` | earnings_surprise_zscore | Alias proxy đã chốt |
| E — Composite | `gold_compose_score.sql` | 4 pillar scores | Gộp 0.25, giữ coverage/provenance |

Momentum trong workspace đã được đổi tên thành `gold_momentum_pillar.sql`; mẫu và refs Composite bên dưới dùng tên này. `gold_compose_score.sql` giữ tên file hiện tại, tên cột output là `composite_score`.

**Cách đọc:** hoàn thiện bốn pillar A–D trước, rồi mẫu E. Mỗi mẫu có bài toán, input/output, SQL và cách kiểm tra. MVP hiện đã chốt earnings-growth proxy cho Quality; xem [runtime guide](gold-runtime-guide.md) cho implementation, tests và coverage tự động.

## 2. Mẫu A — `gold_growth_pillar`

### 2.1 Bài toán

Intermediate đã có `growth_zscore`. Gold chỉ đặt tên cho score của pillar và giữ key/sector. Không tính lại Revenue Growth hay Net Margin.

**Input:** `int_rebalance_universe` + `int_growth_ratio`.

**Output:** `rebalance_date`, `ticker`, `sector`, `growth_zscore`, `growth_score`.

### 2.2 SQL mẫu

```sql
{{ config(materialized='table') }}

WITH growth_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        g.growth_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_growth_ratio') }} g
        ON g.ticker = u.ticker
       AND g.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    growth_zscore,
    growth_zscore AS growth_score
FROM growth_inputs
```

### 2.3 Đọc từng phần

1. `config`: yêu cầu dbt tạo bảng. Đây là Jinja của dbt, không phải SQL thuần của DuckDB.
2. `ref(...)`: tham chiếu model upstream và cho dbt biết dependency. Trong model nên dùng ref thay vì hardcode tên schema/table.
3. `growth_inputs`: CTE gom input. Một CTE ở đây giúp tách phần join khỏi phần output; không phải bảng riêng được lưu.
4. `u`: lưới ticker–tháng quyết định các dòng output.
5. Hai điều kiện `ON`: chỉ ghép Growth của đúng ticker và đúng tháng.
6. `growth_zscore AS growth_score`: pillar có một factor nên score chính bằng factor score.

Nếu `growth_zscore = NULL`, `growth_score` vẫn NULL và dòng vẫn còn. Nếu score = 0, output giữ 0.

### 2.4 Output minh họa và checkpoint

**Dữ liệu giả lập; ba cột key/sector được giữ như input grid.**

| ticker | growth_zscore | growth_score |
| --- | --- | --- |
| AAA | 1.0 | 1.0 |
| BBB | NULL | NULL |
| CCC | 0.0 | 0.0 |

Sau khi tự viết: output đúng 5 columns, mỗi ticker–tháng một dòng; score bằng z-score kể cả NULL/0. Không tính lại Rule of 40 ở Gold. CTE này chỉ chọn score đã có trong Intermediate.

## 3. Mẫu B — `gold_momentum_pillar`

### 3.1 Bài toán

`int_momentum_ratio` đã lấy giá tháng t−1/t−12 từ toàn bộ lịch sử và chuẩn hóa `momentum_zscore`. Gold dùng score này làm Momentum pillar.

**Input:** `int_rebalance_universe` + `int_momentum_ratio`.

**Output đủ 5 columns:** `rebalance_date`, `ticker`, `sector`, `momentum_zscore`, `momentum_score`.

### 3.2 SQL mẫu

```sql
{{ config(materialized='table') }}

WITH momentum_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        m.momentum_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_momentum_ratio') }} m
        ON m.ticker = u.ticker
       AND m.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    momentum_zscore,
    momentum_zscore AS momentum_score
FROM momentum_inputs
```

### 3.3 Đọc logic

1. Grid quyết định các dòng được giữ, cùng với sector.
2. `ref('int_momentum_ratio')` chọn bảng **Intermediate**, dù bảng Gold mang tên pillar.
3. Join đúng ticker và rebalance_date; lịch sử giá trước khi vào index đã được xử lý upstream.
4. Alias `momentum_score` để Composite dùng cùng pattern với các pillar khác.

Không dùng `momentum_12_1_raw` trực tiếp làm pillar score vì raw return chưa cùng thang đo với Growth/Value/Quality z-scores. Không tính lại lookback 12–1, LAG hoặc z-score ở Gold.

### 3.4 Output minh họa và checkpoint

**Dữ liệu giả lập.**

| ticker | momentum_zscore | momentum_score |
| --- | --- | --- |
| AAA | 0.6 | 0.6 |
| BBB | NULL | NULL |
| CCC | -0.5 | -0.5 |

Score âm là tín hiệu tương đối yếu hơn, vẫn là số liệu hợp lệ. Thiếu giá lookback hoặc thiếu sector upstream có thể cho NULL; Gold giữ NULL và không bỏ ticker đó.

Checkpoint: đúng 5 columns, unique ticker–tháng, row count bằng grid khi input unique; `momentum_score` bằng `momentum_zscore`.

## 4. Mẫu C — `gold_value_pillar`

### 4.1 Bài toán

Gộp FCF Yield score và SBC score theo trọng số bằng nhau:

```text
value_score = 0.5 × fcf_yield_zscore + 0.5 × sbc_zscore
```

`sbc_zscore` đã đảo dấu ở Intermediate: SBC thấp cho điểm tốt hơn. Không đảo dấu thêm ở Gold.

**Input:** grid + `int_fcf_yield` + `int_sbc_ratio`.

**Output:** `rebalance_date`, `ticker`, `sector`, `fcf_yield_zscore`, `sbc_zscore`, `value_score`.

### 4.2 SQL mẫu

```sql
{{ config(materialized='table') }}

WITH value_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        f.fcf_yield_zscore,
        s.sbc_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_fcf_yield') }} f
        ON f.ticker = u.ticker
       AND f.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('int_sbc_ratio') }} s
        ON s.ticker = u.ticker
       AND s.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    fcf_yield_zscore,
    sbc_zscore,
    CASE
        WHEN fcf_yield_zscore IS NOT NULL
         AND sbc_zscore IS NOT NULL
        THEN 0.5 * fcf_yield_zscore + 0.5 * sbc_zscore
        ELSE NULL
    END AS value_score
FROM value_inputs
```

### 4.3 Vì sao viết CASE rõ ràng?

Trong SQL, `0.5 * NULL + 0.5 * x` đã trả NULL. CASE ở mẫu giúp bạn đọc thấy ngay quy tắc **cần đủ hai factors**. Sau này thay weighting hoặc thêm factor vẫn dễ kiểm soát missingness.

Không dùng `AVG` trên các factor values nếu nó bỏ qua NULL, vì khi thiếu một factor, score còn lại sẽ vô tình nhận 100% trọng số. Không dùng `COALESCE(score, 0)` để làm Value có điểm khi thiếu input.

### 4.4 Output minh họa

**Dữ liệu giả lập, không phải kết quả ticker thật.**

| ticker | rebalance_date | fcf_yield_zscore | sbc_zscore | value_score |
| --- | --- | --- | --- | --- |
| AAA | 2026-09-30 | 0.8 | 0.4 | 0.6 |
| BBB | 2026-09-30 | NULL | -0.2 | NULL |
| CCC | 2026-09-30 | 0.0 | 0.6 | 0.3 |

AAA có đủ hai scores. BBB vẫn có một dòng nhưng Value thiếu dữ liệu. CCC có FCF score 0 thật, vẫn tính được Value.

## 5. Mẫu D — `gold_quality_pillar` (earnings-growth proxy)

### 5.1 Giả định của mẫu

MVP đã chốt giữ proxy: Net Income growth YoY theo quý, chuẩn hóa thành `earnings_surprise_zscore`, alias thành `quality_score`. Definition ID là `quarterly_net_income_yoy_growth_proxy_v1`; xem [runtime guide](gold-runtime-guide.md) cho flow/tests/coverage hiện đã triển khai.

Đây chưa phải Quality factor kiểu profitability/stability và không phải SUE chuẩn. Nếu bạn đổi định nghĩa Quality sau này, cần thay input/construction tương ứng và cập nhật tài liệu; alias một cột không thay đổi ý nghĩa kinh tế của nó.

**Input:** `int_rebalance_universe` + `int_earnings_surprise`.

**Output đủ 5 columns:** `rebalance_date`, `ticker`, `sector`, `earnings_surprise_zscore`, `quality_score`.

### 5.2 SQL mẫu

```sql
{{ config(materialized='table') }}

WITH quality_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        e.earnings_surprise_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_earnings_surprise') }} e
        ON e.ticker = u.ticker
       AND e.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    earnings_surprise_zscore,
    earnings_surprise_zscore AS quality_score
FROM quality_inputs
```

### 5.3 Đọc logic

1. Hai quý và ngày công bố đã được Intermediate chọn theo availability; Gold không kéo lại BCTC.
2. Giữ tên `earnings_surprise_zscore` để trace về input thật của model.
3. `quality_score` là tên chuẩn của pillar mà Composite sẽ đọc.
4. Giữ hướng score hiện tại: growth proxy cao hơn → pillar score cao hơn; không đảo dấu như SBC.

Không dùng `earnings_surprise_raw` trực tiếp thay cho z-score. Không thêm stddev vào Gold để gọi là SUE; nếu muốn SUE chuẩn, đó là một thay đổi construction ở Intermediate.

### 5.4 Output minh họa và checkpoint

**Dữ liệu giả lập.**

| ticker | earnings_surprise_zscore | quality_score |
| --- | --- | --- |
| AAA | 0.2 | 0.2 |
| BBB | -0.1 | -0.1 |
| CCC | NULL | NULL |

Thiếu quý cùng kỳ, mẫu số bằng 0 hoặc thiếu sector/dispersion có thể dẫn đến score NULL upstream. Gold giữ dòng với NULL, không thay bằng 0.

Checkpoint: đủ 5 columns, unique ticker–tháng, score bằng input z-score; ghi rõ "earnings-growth proxy" trong phần mô tả model nếu bạn chọn dùng mẫu này.

## 6. Mẫu E — `gold_compose_score`

### 6.1 Bài toán

Lấy bốn pillar scores, giữ provenance từ universe và chỉ tính composite khi đủ cả bốn:

```text
composite_score = 0.25 × value_score
                + 0.25 × growth_score
                + 0.25 × momentum_score
                + 0.25 × quality_score
```

**Prerequisite:** bốn pillar models đã có output đúng columns và unique `(ticker, rebalance_date)`. Mẫu này chưa chạy được trong project khi các file pillar còn rỗng.

**Output đủ 13 columns:** `rebalance_date`, `ticker`, `cik`, `sector`, `membership_quality`, `identifier_quality`, `value_score`, `growth_score`, `momentum_score`, `quality_score`, `available_pillar_count`, `composite_score`, `is_score_eligible`.

**Input:** grid + `gold_value_pillar`, `gold_growth_pillar`, `gold_momentum_pillar`, `gold_quality_pillar`.

### 6.2 SQL mẫu

```sql
{{ config(materialized='table') }}

WITH joined_pillars AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.cik,
        u.sector,
        u.membership_quality,
        u.identifier_quality,
        v.value_score,
        g.growth_score,
        m.momentum_score,
        q.quality_score
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('gold_value_pillar') }} v
        ON v.ticker = u.ticker
       AND v.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_growth_pillar') }} g
        ON g.ticker = u.ticker
       AND g.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_momentum_pillar') }} m
        ON m.ticker = u.ticker
       AND m.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_quality_pillar') }} q
        ON q.ticker = u.ticker
       AND q.rebalance_date = u.rebalance_date
),

coverage AS (
    SELECT
        rebalance_date,
        ticker,
        cik,
        sector,
        membership_quality,
        identifier_quality,
        value_score,
        growth_score,
        momentum_score,
        quality_score,
        CASE WHEN value_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN growth_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN momentum_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN quality_score IS NOT NULL THEN 1 ELSE 0 END
            AS available_pillar_count
    FROM joined_pillars
)

SELECT
    rebalance_date,
    ticker,
    cik,
    sector,
    membership_quality,
    identifier_quality,
    value_score,
    growth_score,
    momentum_score,
    quality_score,
    available_pillar_count,
    CASE
        WHEN available_pillar_count = 4
        THEN 0.25 * value_score
           + 0.25 * growth_score
           + 0.25 * momentum_score
           + 0.25 * quality_score
        ELSE NULL
    END AS composite_score,
    available_pillar_count = 4 AS is_score_eligible
FROM coverage
```

### 6.3 Cách đọc ba khối

| Khối | Nó chịu trách nhiệm gì? | Không đưa gì vào khối này? |
| --- | --- | --- |
| joined_pillars | Ghép đúng ticker–tháng, giữ universe/provenance | Không tính lại ratios/as-of facts |
| coverage | Đếm số pillar scores đã có | Không thay missing scores bằng 0 |
| SELECT cuối | Tính composite/eligibility và chọn schema output | Không lọc mất dòng thiếu pillar |

`available_pillar_count` đếm **cột score không NULL trong mỗi dòng**, không phải COUNT(*) số dòng toàn bảng. Bốn CASE trả 0/1 là cờ presence; chúng không sửa giá trị score.

`is_score_eligible` chỉ nói đủ điểm để xếp hạng. Một dòng có đủ bốn scores nhưng `membership_quality=monthly_observation` vẫn là dữ liệu observation; nó không trở thành verified effective-date history.

### 6.4 Output minh họa

**Dữ liệu giả lập.**

| ticker | Value | Growth | Momentum | Quality | count | composite | eligible |
| --- | --- | --- | --- | --- | --- | --- | --- |
| AAA | 0.6 | 1.0 | 0.6 | 0.2 | 4 | 0.6 | true |
| BBB | NULL | 0.4 | 0.2 | -0.1 | 3 | NULL | false |
| CCC | 0.3 | 0.0 | -0.5 | 0.2 | 4 | 0.0 | true |

CCC minh họa composite bằng 0 vẫn là giá trị hợp lệ, không phải thiếu dữ liệu. BBB không chia tổng ba pillar còn lại cho 3.

## 7. Thứ tự bạn tự triển khai

1. Đọc output `int_growth_ratio`; viết Growth theo mẫu A, kiểm tra schema/NULL.
2. Viết Momentum theo mẫu B vào `gold_momentum_pillar.sql`.
3. Đọc FCF/SBC scores; viết Value theo mẫu C, kiểm tra missing input và SBC đã đảo dấu.
4. Chốt Quality proxy, rồi viết theo mẫu D nếu giữ định nghĩa MVP này. Ghi định nghĩa đã chọn trong docs.
5. Viết Composite theo mẫu E sau khi bốn pillar đã build được.
6. Thêm `dbt/models/gold/schema.yml` và data tests; kiểm tra duplicate keys trước join.
7. Sau khi models/gates ổn, thêm Gold task vào DAG.

Đứng trong thư mục dbt, **chỉ sau khi bạn đã viết model tương ứng**, có thể chạy:

```powershell
# Mẫu một factor; + chọn cả upstream cần thiết.
dbt build --select +gold_growth_pillar --vars '{allow_observed_universe: true}'

# Sau khi Momentum có code:
dbt build --select +gold_momentum_pillar --vars '{allow_observed_universe: true}'

# Sau khi Value có code:
dbt build --select +gold_value_pillar --vars '{allow_observed_universe: true}'

# Sau khi Quality proxy đã chốt và có code:
dbt build --select +gold_quality_pillar --vars '{allow_observed_universe: true}'

# Sau khi cả 4 pillars và composite có code:
dbt build --select +gold_compose_score --vars '{allow_observed_universe: true}'
```

Các lệnh ghi vào warehouse của profile. True là chế độ nghiên cứu bằng universe observations hiện có; strict mặc định vẫn cần effective-date history.

## 8. Tự kiểm tra sau mỗi model

### 8.1 Grain và row count

Query đọc minh họa, thay tên bảng khi cần:

```sql
SELECT ticker, rebalance_date, COUNT(*) AS n
FROM gold_value_pillar
GROUP BY 1, 2
HAVING COUNT(*) > 1;
```

Mong đợi **0 dòng**. Khi dùng grid LEFT JOIN mà upstream unique, row count của pillar phải bằng grid. Nếu số dòng tăng, kiểm tra keys upstream/join; không dùng DISTINCT để che lỗi.

### 8.2 Rules cần kiểm chứng

| Model | Mong đợi |
| --- | --- |
| Growth/Momentum/Quality proxy | Pillar score bằng factor score; NULL giữ NULL |
| Value | Một input NULL → Value NULL; cả hai có → khớp 0.5 mỗi factor |
| Composite | count trong 0–4; thiếu một pillar → composite NULL/eligible false |
| Composite | đủ bốn → composite khớp công thức 0.25; eligible true |
| Tất cả | Key không NULL/trùng; dates không bị trộn; provenance từ grid còn giữ |

Z-score upstream bị kẹp [-3, 3], nên pillar/composite theo các trọng số dương tổng bằng 1 cũng nằm trong khoảng này khi có dữ liệu. Khi so sánh số thực nên dùng dung sai, ví dụ absolute difference > 1e-9, không dựa vào equality tuyệt đối.

Sau khi rules pass, đọc coverage theo tháng: bao nhiêu mã có đủ 4 scores, các mã bị thiếu vì factor nào. Gold giữ các mã thiếu dữ liệu để audit; ranking/backtest có thể dùng một view eligible riêng sau đó.

## 9. Bài tập nhỏ để hiểu trước khi viết

- Nếu FCF score thiếu nhưng SBC score là 0.8, Value ra gì? **NULL**, vì thiếu một factor.
- Nếu cả hai Value inputs là 0, có được tính không? **Có**, Value = 0.
- Nếu score cùng ticker có tháng 8 và 9, chỉ join ticker có đúng không? **Không**, cần thêm rebalance_date.
- Nếu đủ bốn pillar nhưng universe là observation, có được gọi PIT chính xác không? **Không**; eligibility score khác quality lịch sử.
- Nếu đủ bốn pillar và composite cao, đã chứng minh alpha chưa? **Chưa**; cần backtest/validation riêng.

Đọc tiếp [Data Dictionary & Lộ trình](Data%20Dictionary.md) để đối chiếu đủ columns và [semantics-and-universe.md](semantics-and-universe.md) để xem quy ước dữ liệu.
