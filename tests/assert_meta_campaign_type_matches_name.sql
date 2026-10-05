-- Facebook campaign_type must match the campaign name (last 90 days). Passes with 0 rows.

SELECT DISTINCT utm_campaign, campaign_type
FROM {{ ref('blended_performance') }}
WHERE channel = 'Facebook'
  AND date_granularity = 'day'
  AND date >= DATEADD(day, -90, CURRENT_DATE)
  AND COALESCE(campaign_type, '') <> {{ erie_meta_campaign_type('utm_campaign') }}
