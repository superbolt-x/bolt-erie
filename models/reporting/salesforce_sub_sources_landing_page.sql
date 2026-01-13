{{ config (
    alias = target.database + '_salesforce_sub_sources_landing_page'
)}}

SELECT CASE WHEN source IN ('SM','SMR','SMO','SM1','SM13','BSM','BSMR','BSM1') OR utm_source = 'facebook' THEN 'Facebook'
            WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN 'YouTube'
            WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') OR utm_source = 'google' THEN 'Google'
            WHEN source = 'SM6' OR utm_source = 'tiktok' THEN 'TikTok'
            WHEN source IN ('IL3','BIL3') OR utm_source = 'bing' THEN 'Bing'
            WHEN source IN ('SM5','BSM5') OR utm_source = 'nextdoor' THEN 'Nextdoor'
            WHEN source = 'SM3' OR utm_source = 'outbrain' THEN 'Outbrain'
            ELSE 'Other'
        END AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        source,
        CASE WHEN source ~* 'B' THEN 'Basement'
            WHEN source !~* 'B' THEN 'Roofing' 
        END as erie_type,
        CASE WHEN source IN ('SMR','SM1','SM4','BSM4','IL3','BIL3','SM3','BSMR','BSM1','SM5','BSM5') THEN 'National'
            WHEN source IN ('SM','SMO','SM2','BSM','BSM2') THEN 'Local'
            WHEN source = 'RYT' THEN 'Retargeting'
            ELSE 'Other'
        END as market,
        CASE WHEN source IN ('SM2','SM4','SM1','SM','BSM','BSM2','BSM1') 
                OR (utm_source = 'facebook' AND utm_campaign !~* 'warm') THEN 'Prospecting'
            WHEN source IN ('SMR','SMO','BSMR')
                OR (utm_source = 'facebook' AND utm_campaign ~* 'warm') THEN 'Retargeting'
            WHEN source IN ('SMD','BSMD') OR (utm_source = 'google' AND utm_campaign ~* 'discovery') OR (utm_source = 'google' AND utm_campaign ~* 'demand gen') THEN 'Demand Gen'
            WHEN source IN ('PMX','BPMX') OR (utm_source = 'google' AND advertising_channel_type = 'PERFORMANCE_MAX') THEN 'Performance Max'
            WHEN source IN ('IL2','BIL2','IL3','BIL3') OR (utm_source = 'google' AND advertising_channel_type = 'SEARCH')THEN 'Search'
        END as campaign_type,
        CASE WHEN bg_campaign_name::VARCHAR ~* 'All areas' OR utm_campaign ~* 'All areas' THEN 'All areas' 
            WHEN bg_campaign_name::VARCHAR ~* 'Group' OR utm_campaign ~* 'Group' THEN 'Group' 
            WHEN bg_campaign_name::VARCHAR ~* 'National' OR utm_campaign ~* 'National' THEN 'National' 
            ELSE 'Other'
        END as region_bucket,
        utm_medium,
        CASE WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') OR utm_source = 'google' THEN bg_campaign_name::VARCHAR
            WHEN source IN ('IL3','BIL3') OR utm_source = 'bing' THEN bg_campaign_name::VARCHAR
            ELSE utm_campaign::VARCHAR
        END as utm_campaign,
        utm_lp_variant,
        0 AS spend,
        0 AS clicks,
        0 AS impressions,
        COALESCE(SUM(leads)::int,0)::int as sf_leads,
        COALESCE(SUM(appointments),0) as appointments,
        COALESCE(SUM(workable_leads),0) as workable_leads
    FROM (SELECT *, COALESCE(utm_campaign_id::VARCHAR,utm_campaign) as utm_campaign_id_adj
            FROM {{ source('reporting','salesforce_performance') }}) s
    LEFT JOIN (SELECT campaign_id::VARCHAR as campaign_id, campaign_name as bg_campaign_name, advertising_channel_type
            FROM {{ ref('googleads_campaigns') }}
            UNION ALL
            SELECT campaign_id::VARCHAR as campaign_id, campaign_name as bg_campaign_name, NULL as advertising_channel_type
            FROM {{ ref('bingads_campaigns') }}
            ) bg ON s.utm_campaign_id_adj = bg.campaign_id
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
