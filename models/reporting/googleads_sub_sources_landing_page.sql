{{ config (
    alias = target.database + '_googleads_sub_sources_landing_page'
)}}

WITH campaign_max_updated_date as 
  (SELECT id , max(updated_at) as max_updated_at FROM {{ source('googleads_raw', 'campaign_history') }} GROUP BY 1),

campaign_types as (
   SELECT campaign_max_updated_date.id as campaign_id, advertising_channel_type
   FROM campaign_max_updated_date 
   LEFT JOIN {{ source('googleads_raw', 'campaign_history') }}
   ON campaign_max_updated_date.id = campaign_history.id AND campaign_max_updated_date.max_updated_at = campaign_history.updated_at),
  
googleads_data as 
    (SELECT 
        date, 
        date_granularity, 
        office, 
        office_location, 
        erie_type,
        market,
        campaign_name,
        campaign_id, 
        ad_group_id,
        ad_group_name,
        landing_page,
        lp_variant,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions
    FROM {{ source('reporting','googleads_landing_page_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12)

SELECT 
        'Google' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as source,
        erie_type,
        market,
        CASE WHEN (advertising_channel_type = 'DISCOVERY' OR campaign_name ~* 'demand gen' OR campaign_name ~* 'discovery') THEN 'Demand Gen'
            WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Performance Max'
            WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen' OR campaign_name ~* 'basements keywords' 
                OR campaign_name ~* 'priority markets' OR campaign_name ~* 'worse cpl locations' OR advertising_channel_type = 'SEARCH' THEN 'Search'
        END as campaign_type,
        CASE WHEN campaign_name ~* 'All areas' THEN 'All areas' 
            WHEN campaign_name ~* 'Group' THEN 'Group' 
            WHEN campaign_name ~* 'National' THEN 'National' 
            ELSE 'Other'
        END as region_bucket,
        CASE WHEN ad_group_name ~* 'Roof Replacement' THEN 'Roof Replacement' 
            WHEN ad_group_name ~* 'General Roofing' THEN 'General Roofing' 
            WHEN ad_group_name ~* 'Residential Roofing' THEN 'Residential Roofing'
            WHEN ad_group_name ~* 'Metal Roofing' THEN 'Metal Roofing' 
            WHEN ad_group_name ~* 'Steel Roofing' THEN 'Steel Roofing'
            WHEN ad_group_name ~* 'Fiberglass Roofing' THEN 'Fiberglass Roofing'
            WHEN ad_group_name ~* 'Spanish Tiles' THEN 'Spanish Tiles'
            ELSE 'Other'
        END as service_type,
        NULL as utm_medium,
        campaign_name::VARCHAR as utm_campaign,
        ad_group_name::VARCHAR as utm_term,
        lp_variant as utm_lp_variant,
        campaign_id::VARCHAR as utm_campaign_id,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        0 as sf_leads,
        0 as appointments,
        0 as workable_leads
    FROM googleads_data 
    LEFT JOIN campaign_types USING(campaign_id)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
