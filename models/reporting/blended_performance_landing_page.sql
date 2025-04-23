{{ config (
    alias = target.database + '_blended_performance_landing_page'
)}}

(SELECT * FROM {{ ref('googleads_sub_sources_landing_page') }})
UNION ALL
(SELECT * FROM {{ ref('bingads_sub_sources_landing_page') }})
UNION ALL 
(SELECT * FROM 
        (SELECT 
        channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        source,
        erie_type,
        market,
        campaign_type,
        region_bucket
        service_type,
        utm_medium,
        utm_campaign,
        utm_term,
        utm_lp_variant,
        0 AS spend,
        0 AS clicks,
        0 AS impressions,
        sf_leads,
        appointments,
        workable_leads
        FROM {{ ref('salesforce_sub_sources') }} 
        WHERE (channel = 'Bing' OR channel = 'Google') )
)
