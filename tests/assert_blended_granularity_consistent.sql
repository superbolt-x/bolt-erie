-- The invariant that R11 restored: a month bucket must equal the days inside it.
--
-- Before the url_split fix in googleads_sub_sources.sql, conversion metrics grew with
-- bucket size because raw_ad_daily is at ad x final-URL grain while googleads_ad_performance
-- is at ad grain, so the join fanned conversions out once per URL. Measured drift on
-- inplatform_leads for Google+YouTube was day 143,427 -> year 162,320 (+13.2%).
--
-- Checks the three most recent complete months. Passes when zero rows are returned.
-- Tolerance is 1% to absorb late-arriving rows between the two reads.

WITH day_rollup AS (
    SELECT channel,
           DATE_TRUNC('month', date) AS month_start,
           SUM(inplatform_leads) AS leads,
           SUM(spend)            AS spend
    FROM {{ ref('blended_performance') }}
    WHERE date_granularity = 'day'
      AND date >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE))
      AND date <  DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2
),

month_rows AS (
    SELECT channel,
           date AS month_start,
           SUM(inplatform_leads) AS leads,
           SUM(spend)            AS spend
    FROM {{ ref('blended_performance') }}
    WHERE date_granularity = 'month'
      AND date >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE))
      AND date <  DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2
)

SELECT d.channel,
       d.month_start,
       d.leads  AS day_leads,
       m.leads  AS month_leads,
       d.spend  AS day_spend,
       m.spend  AS month_spend
FROM day_rollup d
JOIN month_rows m
  ON d.channel = m.channel
 AND d.month_start = m.month_start
WHERE ABS(m.leads - d.leads) / NULLIF(d.leads, 0) > 0.01
   OR ABS(m.spend - d.spend) / NULLIF(d.spend, 0) > 0.01
