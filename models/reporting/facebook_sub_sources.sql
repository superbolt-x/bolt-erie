
SELECT 
        'Facebook' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as sf_locations, 
        NULL as source,
        NULL as sub_source_id, 
        NULL as sub_source,
        NULL as zip, 
        CASE WHEN (account_id = '813620678687014' OR account_id = '306770030564777') THEN 'Roofing' 
             WHEN account_id = '1349056908916556' THEN 'Basement'
        END as erie_type,
        CASE WHEN (campaign_name ~* 'All Area' and (account_id = '813620678687014' OR account_id = '306770030564777') ) or ((campaign_name ~* 'sandbox' or campaign_name ~* 'All Area') and account_id = '1349056908916556') THEN 'National'
             WHEN ((campaign_name !~* 'All Area' and (account_id = '813620678687014' OR account_id = '306770030564777') ) or ((campaign_name !~* 'sandbox' or campaign_name !~* 'All Area') and account_id = '1349056908916556')) AND campaign_name ~* 'Local' THEN 'Local'
        END as market,
        CASE WHEN campaign_name !~* 'warm' THEN 'Prospecting' 
            WHEN campaign_name ~* 'warm' THEN 'Retargeting' 
            WHEN campaign_name ~* 'LP Clicks Traffic' THEN 'Traffic' 
            WHEN campaign_name ~* 'LP Views Leads' THEN 'View Content' 
        END as campaign_type,
        CASE WHEN campaign_name ~* 'All areas' THEN 'All areas' 
            WHEN campaign_name ~* 'Group' THEN 'Group' 
            WHEN campaign_name ~* 'National' THEN 'National' 
            ELSE 'Other'
        END as region_bucket,
        CASE WHEN adset_name ~* 'Roof Replacement' THEN 'Roof Replacement' 
            WHEN adset_name ~* 'General Roofing' THEN 'General Roofing' 
            WHEN adset_name ~* 'Residential Roofing' THEN 'Residential Roofing'
            WHEN adset_name ~* 'Metal Roofing' THEN 'Metal Roofing' 
            WHEN adset_name ~* 'Steel Roofing' THEN 'Steel Roofing'
            WHEN adset_name ~* 'Fiberglass Roofing' THEN 'Fiberglass Roofing'
            WHEN adset_name ~* 'Spanish Tiles' THEN 'Spanish Tiles'
            ELSE 'Other'
        END as service_type,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        campaign_name::VARCHAR as utm_campaign,
        adset_name::VARCHAR as utm_term,
        ad_name::VARCHAR as utm_content,
        NULL as utm_keyword,
        NULL as utm_match_type,
        NULL as utm_placement,
        NULL as utm_discount,
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
        0 as ooa_leads,
        0 AS inplatform_workable_leads,
        0 AS inplatform_appointments
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
