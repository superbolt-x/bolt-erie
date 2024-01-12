{{ config (
    alias = target.database + '_outbrain_campaign_performance'
)}}

SELECT DATE_TRUNC('day',day::date) as date, 'day' as date_granularity,
  campaign_id,
  campaign_name,
  platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(conversions),0) as conversions,
  COALESCE(SUM(sum_value),0) as conversions_value
  --leads
FROM {{ source('outbrain_raw','campaign_report') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4,5

UNION ALL

SELECT DATE_TRUNC('week',day::date) as date, 'week' as date_granularity,
  campaign_id,
  campaign_name,
  platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(conversions),0) as conversions,
  COALESCE(SUM(sum_value),0) as conversions_value
  --leads
FROM {{ source('outbrain_raw','campaign_report') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4,5

UNION ALL

SELECT DATE_TRUNC('month',day::date) as date, 'month' as date_granularity,
  campaign_id,
  campaign_name,
  platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(conversions),0) as conversions,
  COALESCE(SUM(sum_value),0) as conversions_value
  --leads
FROM {{ source('outbrain_raw','campaign_report') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4,5

UNION ALL

SELECT DATE_TRUNC('quarter',day::date) as date, 'quarter' as date_granularity,
  campaign_id,
  campaign_name,
  platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(conversions),0) as conversions,
  COALESCE(SUM(sum_value),0) as conversions_value
  --leads
FROM {{ source('outbrain_raw','campaign_report') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4,5

UNION ALL

SELECT DATE_TRUNC('year',day::date) as date, 'year' as date_granularity,
  campaign_id,
  campaign_name,
  platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(conversions),0) as conversions,
  COALESCE(SUM(sum_value),0) as conversions_value
  --leads
FROM {{ source('outbrain_raw','campaign_report') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4,5
