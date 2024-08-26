{{ config (
    alias = target.database + '_googleads_campaign_hourly_performance'
)}}

WITH initial_data as
(SELECT date, hour, campaign_name, campaign_id, 'Basement' as erie_type, _fivetran_synced,
    COALESCE(SUM(cost),0) as spend, 0 as leads,
    MAX(_fivetran_synced) OVER (PARTITION by date, hour, campaign_name, campaign_id) as last_updated_date
  FROM {{ source('gsheet_raw','campaign_hourly_basement_report') }}
  GROUP BY 1,2,3,4,5,_fivetran_synced
  UNION ALL
  SELECT date, hour, campaign_name, campaign_id, 'Roofing' as erie_type, _fivetran_synced,
    COALESCE(SUM(cost),0) as spend, 0 as leads,
    MAX(_fivetran_synced) OVER (PARTITION by date, hour, campaign_name, campaign_id) as last_updated_date
  FROM {{ source('gsheet_raw','campaign_hourly_roof_report') }}
  GROUP BY 1,2,3,4,5,_fivetran_synced
  UNION ALL
  SELECT date, hour, campaign_name, campaign_id, CASE WHEN customer_id = 4560674777 THEN 'Roofing' WHEN customer_id = 2819798401 THEN 'Basement' END as erie_type, _fivetran_synced,
    0 as spend, COALESCE(SUM(CASE WHEN conversion_action_name ~* '(Kashurba) Get Pricing' THEN all_conversions END),0) as leads,
    MAX(_fivetran_synced) OVER (PARTITION by date, hour, campaign_name, campaign_id) as last_updated_date
  FROM {{ source('gsheet_raw','campaign_hourly_roof_report') }}
  GROUP BY 1,2,3,4,5,_fivetran_synced)
  
  final_data as 
  (SELECT * FROM initial_data WHERE _fivetran_synced = last_updated_date)

  SELECT 
    date,
    hour,
    campaign_id,
    campaign_name,
    erie_type,
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(leads),0) as leads
  FROM final_data
  GROUP BY 1,2,3,4,5
