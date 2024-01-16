{{ config (
    alias = target.database + '_outbrain_campaign_performance'
)}}

SELECT DATE_TRUNC('day',date::date) as date, 'day' as date_granularity,
  campaign_id,
  campaign_name,
  --platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(total_conversions),0) as conversions,
  COALESCE(SUM(total_conversion_value),0) as conversions_value,
  COALESCE(SUM(total_erie_lead),0) as leads
FROM {{ source('gsheet_raw','outbrain_campaign_insights') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('week',date::date) as date, 'week' as date_granularity,
  campaign_id,
  campaign_name,
  --platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(total_conversions),0) as conversions,
  COALESCE(SUM(total_conversion_value),0) as conversions_value,
  COALESCE(SUM(total_erie_lead),0) as leads
FROM {{ source('gsheet_raw','outbrain_campaign_insights') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('month',date::date) as date, 'month' as date_granularity,
  campaign_id,
  campaign_name,
  --platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(total_conversions),0) as conversions,
  COALESCE(SUM(total_conversion_value),0) as conversions_value,
  COALESCE(SUM(total_erie_lead),0) as leads
FROM {{ source('gsheet_raw','outbrain_campaign_insights') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('quarter',date::date) as date, 'quarter' as date_granularity,
  campaign_id,
  campaign_name,
  --platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(total_conversions),0) as conversions,
  COALESCE(SUM(total_conversion_value),0) as conversions_value,
  COALESCE(SUM(total_erie_lead),0) as leads
FROM {{ source('gsheet_raw','outbrain_campaign_insights') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('year',date::date) as date, 'year' as date_granularity,
  campaign_id,
  campaign_name,
  --platform,
  --publisher,
  --audience,
  --location,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions,
  COALESCE(SUM(total_conversions),0) as conversions,
  COALESCE(SUM(total_conversion_value),0) as conversions_value,
  COALESCE(SUM(total_erie_lead),0) as leads
FROM {{ source('gsheet_raw','outbrain_campaign_insights') }}
LEFT JOIN (SELECT id as campaign_id, name as campaign_name FROM {{ source('outbrain_raw','campaign_history') }} ) USING(campaign_id)
GROUP BY 1,2,3,4
