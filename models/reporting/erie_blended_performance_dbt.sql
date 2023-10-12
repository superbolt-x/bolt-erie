WITH office_data as
    (SELECT office as sf_office, 
        case 
            WHEN LEFT(office,1)='R' THEN SPLIT_PART(SPLIT_PART(office,' ',1),'R',2) 
            WHEN LEFT(office,1)='B'THEN SPLIT_PART(office,' ',1)
        end as code, 
        SPLIT_PART(office,' ',2) + SPLIT_PART(office,' ',3) + SPLIT_PART(office,' ',4) as location
    FROM gsheet_raw.office_locations
    GROUP BY office
    ORDER BY code ASC)

    (SELECT 'Facebook' AS channel, date, date_granularity, CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, sf_office as office_location, NULL as sf_locations, NULL as sub_source_id, NULL as sub_source,
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
        COALESCE(SUM(leads),0) AS inplatform_leads,
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
    FROM reporting.erie_facebook_ad_performance
    LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_effective_status, 
        case 
            when (account_id = '813620678687014' OR account_id = '306770030564777') then RIGHT(LEFT(campaign_name,4),3) 
            when account_id = '1349056908916556' AND LEFT(campaign_name,4) = 'B071' THEN 'B001'
            when account_id = '1349056908916556' AND LEFT(campaign_name,4) = 'B078' THEN 'B002'
            when account_id = '1349056908916556' THEN LEFT(campaign_name,4)
        end as code 
        FROM facebook_base.facebook_campaigns) c
        USING(campaign_id, campaign_name, account_id, campaign_effective_status)
    LEFT JOIN office_data USING(code)
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)

    UNION ALL
    
    (SELECT 'Google' AS channel, date, date_granularity, CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, sf_office as office_location, NULL as sf_locations, sub_source_id, sub_source,
        NULL as zip, erie_type, market, 
        CASE WHEN campaign_name ~* 'Discovery' THEN 'Discovery'
            WHEN campaign_name ~* 'Performance Max' OR campaign_name ~* 'PMAX' THEN 'Performance Max'
            WHEN 
                (campaign_name ~* 'Branded' 
                OR campaign_name ~* 'Metal Roofing Keywords' 
                OR campaign_name ~* 'Basement Keywords' 
                OR advertising_channel_type = 'SEARCH')
            THEN 'Search'
        END as campaign_type,
        campaign_name,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(leads),0) AS inplatform_leads,
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
    FROM (SELECT * FROM {{ ref('erie_googleads_sub_source_for_blended') }} WHERE campaign_type_default != 'Campaign Type: Youtube')
    LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status, advertising_channel_type,  
            case 
                when account_id = '4560674777' THEN RIGHT(LEFT(campaign_name,4),3) 
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0071' THEN 'B001'
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0078' THEN 'B002'
                when account_id = '2819798401' THEN LEFT(campaign_name,4)
            end as code 
                FROM googleads_base.googleads_campaigns) USING(campaign_id, campaign_name, account_id, campaign_status)
    LEFT JOIN office_data USING(code)
    WHERE date >= '2023-05-01'
    AND advertising_channel_type != 'VIDEO'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)

    
    UNION ALL
    
    (SELECT 'YouTube' AS channel, date, date_granularity, CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, sf_office as office_location,  NULL as sf_locations, sub_source_id, sub_source,
        NULL as zip, erie_type, 
        CASE WHEN campaign_name ~* 'all areas' THEN 'National'
            WHEN campaign_name ~* 'warm' THEN 'Retargeting'
            WHEN campaign_name !~* 'all areas' OR campaign_name !~* 'warm' THEN 'Local'
        END as market,
        CASE WHEN campaign_name ~* 'cold' THEN 'Prospecting' 
            WHEN campaign_name ~* 'warm' THEN 'Retargeting'
        END as campaign_type,
        campaign_name,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(leads),0) AS inplatform_leads,
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
    FROM (SELECT * FROM {{ ref('erie_googleads_sub_source_for_blended') }} WHERE campaign_type_default = 'Campaign Type: Youtube')
    LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status, advertising_channel_type,  
                case 
                    when account_id = '4560674777' THEN RIGHT(LEFT(campaign_name,4),3) 
                    when account_id = '2819798401' AND LEFT(campaign_name,4) = '0071' THEN 'B001'
                    when account_id = '2819798401' AND LEFT(campaign_name,4) = '0078' THEN 'B002'
                    when account_id = '2819798401' THEN LEFT(campaign_name,4)
                end as code 
                FROM googleads_base.googleads_campaigns) USING(campaign_id, campaign_name, account_id, campaign_status)
    LEFT JOIN office_data USING(code)
    WHERE date >= '2023-05-01'
    AND advertising_channel_type = 'VIDEO'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    
    UNION ALL
    
    (SELECT * FROM {{ ref('erie_tiktok_sub_source_for_blended') }})
    
    UNION ALL
    
    (SELECT 'Bing' AS channel, date, date_granularity, office, office_location, NULL as sf_locations, NULL as zip, 'Roofing' as erie_type, 'National' as market, NULL as sub_source_id, NULL as sub_source,
        CASE WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'Metal Roofing Keywords' THEN 'Search'
            ELSE 'Other' 
        END as campaign_type,
        campaign_name,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(leads),0) AS inplatform_leads,
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
    FROM reporting.erie_bingads_campaign_performance
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    
    UNION ALL 
    
    (SELECT CASE WHEN source IN ('SM','SMR','SMO','SM1','SM13','BSM','BSMR') THEN 'Facebook'
            WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') THEN 'YouTube'
            WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') THEN 'Google'
            WHEN source = 'SM6' THEN 'TikTok'
            WHEN source IN ('IL3','BIL3') THEN 'Bing'
            ELSE 'Other'
        END AS channel, 
        date, date_granularity, location as office, sf_office as office_location, CASE WHEN market IS NULL THEN '999 - Invalid' ELSE market END as sf_locations, sub_source_id, sub_source, zip,
        CASE WHEN source ~* 'B' THEN 'Basement'
            WHEN source !~* 'B' THEN 'Roofing' 
        END as erie_type,
        CASE WHEN source IN ('SMR','SM1','SM4','BSM4','IL3','BIL3') THEN 'National'
            WHEN source IN ('SM','SMO','SM2','BSM','BSM2') THEN 'Local'
            WHEN source = 'RYT' THEN 'Retargeting'
            ELSE 'Other'
        END as market,
        CASE WHEN source IN ('SM2','SM4','SM1','SM','BSM','BSM2') THEN 'Prospecting'
            WHEN source IN ('SMR','SMO','BSMR') THEN 'Retargeting'
            WHEN source IN ('SMD','BSMD') THEN 'Discovery'
            WHEN source IN ('PMX','BPMX') THEN 'Performance Max'
            WHEN source IN ('IL2','BIL2','IL3','BIL3') THEN 'Search'
        END as campaign_type,
        NULL as campaign_name,
        dispo,
        call_disposition,
        status_detail,
        0 AS spend,
        0 AS clicks,
        0 AS inplatform_leads,
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
    FROM reporting.erie_salesforce_performance 
    LEFT JOIN office_data ON erie_salesforce_performance.market = office_data.sf_office
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    
