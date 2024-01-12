{{ config (
    alias = target.database + '_outbrain_sub_sources'
)}}

SELECT 'Outbrain' AS channel, 
        date, 
        date_granularity, 
        NULL::VARCHAR(256) as office, 
        NULL::VARCHAR(256) as office_location, 
        NULL::VARCHAR(256) as sf_locations, 
        NULL::VARCHAR(256) as sub_source_id, 
        NULL::VARCHAR(256) as sub_source, 
        NULL::VARCHAR(256) as zip, 
        'Roofing' as erie_type,
        'National' as market,
        NULL::VARCHAR(256) as campaign_type,
        campaign_name,
        NULL::VARCHAR(256) as dispo,
        NULL::VARCHAR(256) as call_disposition,
        NULL::VARCHAR(256) as status_detail,
        COALESCE(SUM(spends),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        0 AS inplatform_leads,
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
    FROM {{ source('reporting','outbrain_campaign_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
