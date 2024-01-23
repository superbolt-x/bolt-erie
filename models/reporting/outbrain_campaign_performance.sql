{{ config (
    alias = target.database + '_outbrain_campaign_performance'
)}}

WITH campaign_data as 
    (SELECT id, name as campaign_name, last_modified, max(last_modified) over (partition by id) as last_updated_date FROM {{ source('outbrain_raw','campaign_history') }} )
    , campaign_names as (SELECT id as campaign_id, campaign_name FROM campaign_data WHERE last_modified = last_updated_date)
    ,  campaign_insights_data as 
    (SELECT *, max(api_sync_timestamp) over (partition by campaign_id,date) as last_updated_date FROM {{ source('gsheet_raw','outbrain_campaign_insights') }} )
    ,  campaign_insights as 
    (SELECT * FROM campaign_insights_data where api_sync_timestamp=last_updated_date )

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
FROM campaign_insights
LEFT JOIN campaign_names USING(campaign_id)
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
FROM campaign_insights
LEFT JOIN campaign_names USING(campaign_id)
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
FROM campaign_insights
LEFT JOIN campaign_names USING(campaign_id)
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
FROM campaign_insights
LEFT JOIN campaign_names USING(campaign_id)
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
FROM campaign_insights
LEFT JOIN campaign_names USING(campaign_id)
GROUP BY 1,2,3,4
