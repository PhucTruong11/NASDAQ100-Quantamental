SELECT ticker, count(*) AS n
FROM {{ ref('stg_sector') }}
GROUP BY ticker
HAVING count(*) > 1
