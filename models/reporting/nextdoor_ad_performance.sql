{{ config (
    alias = target.database + '_nextdoor_ad_performance'
)}}
    
{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_data as
  (SELECT *, start_time::date as date, {{ get_date_parts('date') }} 
  FROM {{ source('s3_raw','nextdoor_daily_performance') }} 
  WHERE _modified IN (SELECT MAX(_modified) FROM {{ source('s3_raw','nextdoor_daily_performance') }} )),

  final_data as
  ({%- for date_granularity in date_granularity_list %}    
  SELECT 
      '{{date_granularity}}' as date_granularity,
      {{date_granularity}} as date,
      ad_id,
      ad_name,
      ad_group_id,
      ad_group_name,
      campaign_id,
      campaign_name,
      CASE WHEN campaign_name !~* 'warm' THEN 'Campaign Type: Prospecting' 
        WHEN campaign_name ~* 'warm' THEN 'Campaign Type: Retargeting' 
      END as campaign_type_default,
      COALESCE(SUM(spend),0) as spend,
      COALESCE(SUM(clicks),0) as clicks,
      COALESCE(SUM(impressions),0) as impressions,
      COALESCE(SUM(conversions),0) as conversions
  FROM initial_data
  GROUP BY 1,2,3,4,5,6,7,8,9
  {% if not loop.last %}UNION ALL
  {% endif %}
{% endfor %})

SELECT 
    date,
    date_granularity,
    ad_id,
    ad_name,
    ad_group_id,
    ad_group_name,
    campaign_id,
    campaign_name,
    spend,
    impressions,
    clicks,
    conversions
FROM final_data
