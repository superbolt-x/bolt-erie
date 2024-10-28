{{ config (
    alias = target.database + '_salesforce_sub_sources'
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
        CASE WHEN market IS NULL THEN '999 - Invalid' ELSE market END as sf_locations, 
        source,
        sub_source_id, 
        sub_source, 
        zip,
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
        CASE WHEN gb_ad_group_name::VARCHAR ~* 'Roof Replacement' OR utm_term ~* 'Roof Replacement' THEN 'Roof Replacement' 
            WHEN gb_ad_group_name::VARCHAR ~* 'General Roofing' OR utm_term ~* 'General Roofing' THEN 'General Roofing' 
            WHEN gb_ad_group_name::VARCHAR ~* 'Residential Roofing' OR utm_term ~* 'Residential Roofing' THEN 'Residential Roofing'
            WHEN gb_ad_group_name::VARCHAR ~* 'Metal Roofing' OR utm_term ~* 'Metal Roofing' THEN 'Metal Roofing' 
            WHEN gb_ad_group_name::VARCHAR ~* 'Steel Roofing' OR utm_term ~* 'Steel Roofing' THEN 'Steel Roofing'
            WHEN gb_ad_group_name::VARCHAR ~* 'Fiberglass Roofing' OR utm_term ~* 'Fiberglass Roofing' THEN 'Fiberglass Roofing'
            WHEN gb_ad_group_name::VARCHAR ~* 'Spanish Tiles' OR utm_term ~* 'Spanish Tiles' THEN 'Spanish Tiles'
            ELSE 'Other'
        END as service_type,
        dispo,
        call_disposition,
        status_detail,
        utm_medium,
        CASE WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') OR utm_source = 'google' THEN bg_campaign_name::VARCHAR
            WHEN source IN ('IL3','BIL3') OR utm_source = 'bing' THEN bg_campaign_name::VARCHAR
            ELSE utm_campaign::VARCHAR
        END as utm_campaign,
        CASE WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') OR utm_source = 'google' THEN gb_ad_group_name::VARCHAR
            WHEN source IN ('IL3','BIL3') OR utm_source = 'bing' THEN gb_ad_group_name::VARCHAR
            ELSE utm_term::VARCHAR
        END as utm_term,
        utm_content::VARCHAR,
        utm_keyword,
        utm_match_type,
        utm_placement,
        utm_discount,
        0 AS spend,
        0 AS clicks,
        0 AS impressions,
        0 AS inplatform_leads,
        0 as video_views,
        COALESCE(SUM(leads)::int,0)::int as sf_leads,
        COALESCE(SUM(calls),0) as calls,
        COALESCE(SUM(appointments),0) as appointments,
        COALESCE(SUM(demos),0) as demos,
        COALESCE(SUM(down_payments),0) as down_payments,
        COALESCE(SUM(closed_deals),0) as closed_deals,
        COALESCE(SUM(gross),0) as gross,
        COALESCE(SUM(net),0) as net,
        COALESCE(SUM(workable_leads),0) as workable_leads,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issues),0) as issues,
        COALESCE(SUM(ooa_leads),0) as ooa_leads
    FROM {{ source('reporting','salesforce_performance') }} s
    LEFT JOIN (SELECT campaign_id::VARCHAR as campaign_id, campaign_name as bg_campaign_name, advertising_channel_type
            FROM {{ ref('googleads_campaigns') }}
            UNION ALL
            SELECT campaign_id::VARCHAR as campaign_id, campaign_name as bg_campaign_name, NULL as advertising_channel_type
            FROM {{ ref('bingads_campaigns') }}
            ) bg ON s.utm_campaign = bg.campaign_id
    LEFT JOIN (SELECT ad_group_id::VARCHAR as ad_group_id, ad_group_name as gb_ad_group_name
            FROM {{ ref('googleads_ad_groups') }}
            UNION ALL
            SELECT ad_group_id::VARCHAR as ad_group_id, ad_group_name as gb_ad_group_name
            FROM {{ ref('bingads_ad_groups') }}
            UNION ALL
            SELECT asset_group_id::VARCHAR as ad_group_id, asset_group_name as gb_ad_group_name
            FROM {{ ref('googleads_asset_groups') }}
            ) gb ON s.utm_term = gb.ad_group_id
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
