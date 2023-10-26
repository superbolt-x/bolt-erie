
SELECT 
        'Facebook' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as sf_locations, 
        NULL as sub_source_id, 
        NULL as sub_source,
        NULL as zip, 
        CASE WHEN (account_id = '813620678687014' OR account_id = '306770030564777') THEN 'Roofing' 
            WHEN account_id = '1349056908916556' THEN 'Basement'
        END as erie_type,
        CASE WHEN (campaign_name ~* 'National' OR campaign_name ~* 'All Areas') THEN 'National'
            WHEN (campaign_name !~* 'National' AND campaign_name !~* 'All Areas') OR campaign_name ~* 'Consolidation' THEN 'Local'
        END as market,
        CASE WHEN campaign_name ~* 'cold' THEN 'Prospecting' 
            WHEN campaign_name ~* 'warm' THEN 'Retargeting' 
            WHEN campaign_name ~* 'LP Clicks Traffic' THEN 'Traffic' 
            WHEN campaign_name ~* 'LP Views Leads' THEN 'View Content' 
        END as campaign_type,
        campaign_name,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(link_clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(leads),0) AS inplatform_leads,
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
        0 as ooa_leads
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
