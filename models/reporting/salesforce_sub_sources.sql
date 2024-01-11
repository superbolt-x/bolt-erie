{{ config (
    alias = target.database + '_salesforce_sub_sources'
)}}

SELECT CASE WHEN source IN ('SM','SMR','SMO','SM1','SM13','BSM','BSMR') THEN 'Facebook'
            WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') THEN 'YouTube'
            WHEN source IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD') THEN 'Google'
            WHEN source = 'SM6' THEN 'TikTok'
            WHEN source IN ('IL3','BIL3') THEN 'Bing'
            WHEN source IN ('SM5','BSM5') THEN 'Nextdoor'
            WHEN source = 'SM3' THEN 'Outbrain'
            ELSE 'Other'
        END AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location, 
        CASE WHEN market IS NULL THEN '999 - Invalid' ELSE market END as sf_locations, 
        sub_source_id, 
        sub_source, 
        zip,
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
    FROM {{ source('reporting','salesforce_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
