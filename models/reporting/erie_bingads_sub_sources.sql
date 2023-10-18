WITH sub_source_data as (
SELECT ad_group_id,
        right(ad_group_name,3)::varchar as sub_source_id,
        count(*)
    FROM {{ source ('reporting','erie_bingads_ad_performance') }}
    WHERE date >= '2023-05-01'
    GROUP BY 1,2)

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
    FROM {{ source ('reporting','erie_salesforce_performance') }}
    WHERE source IN ('IL3','BIL3')
    GROUP BY 1,2,3,4)

SELECT 'Bing' AS channel, date, date_granularity, office, office_location, NULL as sf_locations, 
        NULL as zip, 'Roofing' as erie_type, 'National' as market, sub_source_id, sub_source,
        CASE WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'Metal Roofing Keywords' THEN 'Search'
            ELSE 'Other' 
        END as campaign_type,
        campaign_name,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(leads),0) AS inplatform_leads,
        COALESCE(SUM(sf_leads),0) sf_leads,
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
    FROM {{ source ('reporting','erie_bingads_ad_performance') }}
    LEFT JOIN sub_source_data USING(ad_group_id)
    LEFT JOIN sf_data USING(date,date_granularity,sub_source_id)
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
