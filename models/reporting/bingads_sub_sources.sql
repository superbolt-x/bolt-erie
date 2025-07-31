{{ config (
    alias = target.database + '_bingads_sub_sources'
)}}

WITH sub_source_data as (
SELECT ad_group_id,
        right(ad_group_name,3)::varchar as sub_source_id,
        count(*)
    FROM {{ source('reporting','bingads_keyword_performance') }}
    WHERE date >= '2022-12-01'
    GROUP BY 1,2)

, bingads_data as 
    (SELECT 
        date, 
        date_granularity, 
        office, 
        office_location, 
        erie_type,
        campaign_name,
        campaign_id, 
        ad_id,
        ad_group_id,
        ad_group_name,
        keyword_id,
        keyword_name,
        keyword_match_type,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(leads),0) AS inplatform_leads
    FROM {{ source('reporting','bingads_keyword_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)

, joined_data as 
    (SELECT 
        date, 
        date_granularity, 
        office, 
        office_location, 
        erie_type,
        campaign_name,
        campaign_id, 
        ad_id,
        ad_group_id,
        ad_group_name,
        sub_source_id,
        keyword_id,
        keyword_name,
        keyword_match_type,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(inplatform_leads),0) AS inplatform_leads
    FROM bingads_data left join sub_source_data USING(ad_group_id)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14)

, sf_data as 
    (SELECT date, date_granularity, sub_source_id, sub_source,
        COALESCE(SUM(leads),0) sf_leads,
        COALESCE(SUM(calls),0) calls,
        COALESCE(SUM(appointments),0) appointments,
        COALESCE(SUM(demos),0) demos,
        COALESCE(SUM(down_payments),0) down_payments,
        COALESCE(SUM(closed_deals),0) closed_deals,
        COALESCE(SUM(gross),0) gross,
        COALESCE(SUM(net),0) net,
        COALESCE(SUM(workable_leads),0) workable_leads,
        COALESCE(SUM(hits),0) hits,
        COALESCE(SUM(issues),0) issues,
        COALESCE(SUM(ooa_leads),0) ooa_leads
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE source IN ('IL3','BIL3')
    GROUP BY 1,2,3,4)
    

SELECT 
        'Bing' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as sf_locations,
        NULL as source,
        sub_source_id, 
        sub_source,
        NULL as zip,
        erie_type,
        'National' as market,
        CASE WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'Metal Roofing Keywords' THEN 'Search'
            ELSE 'Other' 
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
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        campaign_name::VARCHAR as utm_campaign,
        ad_group_name::VARCHAR as utm_term,
        ad_id::VARCHAR as utm_content,
        keyword_id::VARCHAR as utm_keyword,
        keyword_match_type as utm_match_type,
        NULL as utm_placement,
        NULL as utm_discount,
        NULL as utm_lp_variant,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(inplatform_leads),0) AS inplatform_leads,
        0 as video_views,
        0 as sf_leads,
        0 as calls,
        0 as appointments,
        0 as demos,
        0 as down_payments,
        0 as closed_deals,
        0 as gross,
        0 as net,
        0 as workable_leads,
        0 as hits,
        0 as issues,
        0 as ooa_leads,
        0 AS inplatform_workable_leads,
        0 AS inplatform_appointments
    FROM joined_data LEFT JOIN sf_data USING(date,date_granularity,sub_source_id)
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
