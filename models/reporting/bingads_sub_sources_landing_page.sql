{{ config (
    alias = target.database + '_bingads_sub_sources_landing_page'
)}}

WITH bingads_data as 
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
    FROM {{ source('reporting','bingads_landing_page_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10)

SELECT 
        'Bing' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as source,
        erie_type,
        market,
        CASE WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'Metal Roofing Keywords' THEN 'Search'
            ELSE 'Other' 
        END as campaign_type,
        CASE WHEN campaign_name ~* 'All areas' THEN 'All areas' 
            WHEN campaign_name ~* 'Group' THEN 'Group' 
            WHEN campaign_name ~* 'National' THEN 'National' 
            ELSE 'Other'
        END as region_bucket,
        NULL as utm_medium,
        campaign_name::VARCHAR as utm_campaign,
        lp_variant as utm_lp_variant,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        0 as sf_leads,
        0 as appointments,
        0 as workable_leads
    FROM bingads_data 
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
