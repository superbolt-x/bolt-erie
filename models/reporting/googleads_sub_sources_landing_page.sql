{{ config (
    materialized = 'ephemeral'
)}}

WITH googleads_data as
    (SELECT 
        date, 
        date_granularity, 
        office, 
        office_location, 
        erie_type,
        market,
        campaign_name,
        campaign_id, 
        landing_page,
        lp_variant,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions
    FROM {{ source('reporting','googleads_landing_page_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10)

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
        {{ region_bucket('campaign_name') }} as region_bucket,
        NULL as utm_medium,
        campaign_name::VARCHAR as utm_campaign,
        lp_variant as utm_lp_variant,
        NULL as utm_msclk_id,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        0 as sf_leads,
        0 as appointments,
        0 as workable_leads
    FROM googleads_data
    LEFT JOIN {{ ref('googleads_campaign_types') }} USING(campaign_id)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
