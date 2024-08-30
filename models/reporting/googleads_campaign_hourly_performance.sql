{{ config (
    alias = target.database + '_googleads_campaign_hourly_performance'
)}}

WITH initial_data as
(SELECT date, hour, campaign_name, campaign_id, 'Basement' as erie_type, 
    COALESCE(SUM(cost),0) as spend, 0 as leads
  FROM {{ source('gsheet_raw','campaign_hourly_basement_report') }}
  GROUP BY 1,2,3,4,5
  UNION ALL
  SELECT date, hour, campaign_name, campaign_id, 'Roofing' as erie_type, 
    COALESCE(SUM(cost),0) as spend, 0 as leads
  FROM {{ source('gsheet_raw','campaign_hourly_roof_report') }}
  GROUP BY 1,2,3,4,5
  UNION ALL
  SELECT date, hour, campaign_name, campaign_id, CASE WHEN customer_id = 4560674777 THEN 'Roofing' WHEN customer_id = 2819798401 THEN 'Basement' END as erie_type, 
    0 as spend, COALESCE(SUM(CASE WHEN conversion_action_name = '(Kashurba) Get Pricing' THEN all_conversions END),0) as leads
  FROM {{ source('gsheet_raw','campaign_hourly_convtype_report') }}
  GROUP BY 1,2,3,4,5)

  SELECT 
    date,
    hour,
    campaign_id,
    campaign_name,
    erie_type,
    CASE WHEN campaign_name ~* 'demand gen' OR campaign_name ~* 'discovery' THEN 'Campaign Type: Demand Gen'
        WHEN campaign_name ~* 'pmax' OR campaign_name ~* 'Performance Max' THEN 'Campaign Type: Performance Max'
        WHEN campaign_name ~* 'Youtube' THEN 'Campaign Type: Youtube'
        ELSE 'Campaign Type: Search'
    END as campaign_type,
    CASE WHEN date = current_date THEN 'Today'
        WHEN date != current_date THEN 'Yesterday'
    END as period,
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(leads),0) as leads
  FROM initial_data
  GROUP BY 1,2,3,4,5,6,7
