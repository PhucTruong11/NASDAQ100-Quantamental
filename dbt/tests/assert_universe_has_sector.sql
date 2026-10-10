-- Mọi mã trong universe phải có sector (nếu không z-score sector-neutral mất mã đó)
SELECT m."Ticker"
FROM {{ ref('bronze_index_membership') }} m
LEFT JOIN {{ ref('stg_sector') }} s ON m."Ticker" = s.ticker
WHERE s.ticker IS NULL
