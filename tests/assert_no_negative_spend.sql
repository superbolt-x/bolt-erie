-- Spend should never be negative anywhere in the blended table. A negative value means a
-- refund or clawback has leaked through from a platform loader, and it silently drags down
-- every rate computed over the affected window.
--
-- Passes when zero rows are returned.

SELECT
    channel,
    erie_type,
    date_granularity,
    date,
    SUM(spend) AS spend
FROM {{ ref('blended_performance') }}
GROUP BY 1, 2, 3, 4
HAVING SUM(spend) < 0
