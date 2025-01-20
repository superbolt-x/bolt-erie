{{ config (
    alias = target.database + '_nextdoor_sub_sources'
)}}

SELECT 'Nextdoor' AS channel, 
        date, 
        date_granularity, 
        NULL::VARCHAR(256) as office, 
        NULL::VARCHAR(256) as office_location, 
        NULL::VARCHAR(256) as sf_locations, 
        NULL::VARCHAR(256) as source,
        NULL::VARCHAR(256) as sub_source_id, 
        NULL::VARCHAR(256) as sub_source, 
        NULL::VARCHAR(256) as zip, 
        CASE WHEN campaign_name ~* 'Basement' THEN 'Basement' ELSE 'Roofing' END as erie_type,
        'National' as market,
        campaign_type_default as campaign_type,
        CASE WHEN campaign_name ~* 'All areas' THEN 'All areas' 
            WHEN campaign_name ~* 'Group' THEN 'Group' 
            WHEN campaign_name ~* 'National' THEN 'National' 
            ELSE 'Other'
        END as region_bucket,
        CASE WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Roof_Replacement' THEN 'Roof Replacement' 
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'General_Roofing' THEN 'General Roofing' 
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Residential_Roofing' THEN 'Residential Roofing'
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Metal_Roofing' THEN 'Metal Roofing' 
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Steel_Roofing' THEN 'Steel Roofing'
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Fiberglass_Roofing' THEN 'Fiberglass Roofing'
            WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Spanish_Tiles' THEN 'Spanish Tiles'
            ELSE 'Other'
        END as service_type,
        NULL::VARCHAR(256) as dispo,
        NULL::VARCHAR(256) as call_disposition,
        NULL::VARCHAR(256) as status_detail,
        NULL as utm_medium,
        CASE WHEN campaign_name::VARCHAR ~* ':' THEN TRIM(REPLACE(campaign_name,':','')) ELSE campaign_name::VARCHAR END as utm_campaign,
        CASE WHEN TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR ~* 'Broad_Newsfeed' THEN 'Broad_Newsfeed' 
            ELSE TRIM(REPLACE(REPLACE(ad_group_name,' - ','_'),' ','_'))::VARCHAR
        END as utm_term,
        CASE WHEN TRIM(REPLACE(REPLACE(REPLACE(ad_name,'Copy of ',''),'-',' '),' ','_'))::VARCHAR ~* 'Old_Damaged_Roofs' THEN 'Old_Damaged_Roof' 
            ELSE TRIM(REPLACE(REPLACE(REPLACE(ad_name,'Copy of ',''),'-',' '),' ','_'))::VARCHAR
        END as utm_content,
        NULL as utm_keyword,
        NULL as utm_match_type,
        NULL as utm_placement,
        NULL as utm_discount,
        COALESCE(SUM(spend),0) AS spend,
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
        0 as ooa_leads,
        0 AS inplatform_workable_leads,
        0 AS inplatform_appointments
    FROM {{ source('reporting','nextdoor_ad_performance') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
