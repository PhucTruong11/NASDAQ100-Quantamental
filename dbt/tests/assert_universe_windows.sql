WITH windows AS (
    SELECT *, LAG(valid_to) OVER (PARTITION BY ticker ORDER BY valid_from) AS previous_end,
        ROW_NUMBER() OVER (PARTITION BY ticker, valid_from ORDER BY valid_to) AS rn
    FROM {{ ref('silver_universe_membership_pit') }}
)
SELECT * FROM windows
WHERE valid_from IS NULL OR valid_to <= valid_from
   OR previous_end > valid_from OR rn > 1
