{{ config (
    materialized = 'ephemeral'
)}}

WITH joined_data as  (
    
        (SELECT NULL as ad_final_urls,
                NULL as sub_source_id,
                keyword_id,
                keyword_text,
                case 
                    when keyword_match_type = 'BROAD' then 'b'
                    when keyword_match_type = 'PHRASE' then 'p'
                    when keyword_match_type = 'EXACT' then 'e'
                end as keyword_match_type,
                NULL as ad_id,
                ad_group_id::VARCHAR,
                ad_group_name,
                campaign_name,
                date, 
                campaign_id,
                campaign_type_default,
                advertising_channel_type,
                date_granularity, 
                erie_type, 
                market, 
                NULL as sub_source,
                office,
                office_location,
                spend, 
                clicks, 
                impressions,
                regular_leads,
                purchases,
                video_views,
                workable_leads,
                appointments,
                issues,
                net_sales,
                net_sales_value,
                appointments_value,
                leads,
                account_id,
                campaign_status
        FROM {{ source('reporting','googleads_keyword_performance') }}
        left join {{ ref('googleads_campaign_types') }} USING(campaign_id)
        where date >= '2022-12-01'
        and advertising_channel_type = 'SEARCH')
        
        
    ),
        
    
final_data as (
select 
    account_id,
    keyword_id,
    keyword_text,
    keyword_match_type,
    ad_id,
    ad_group_id,
    ad_group_name,
    campaign_name, 
    campaign_id, 
    campaign_type_default,
    advertising_channel_type,
    campaign_status,
    sub_source_id, 
    sub_source, 
    erie_type, 
    market,
    office,
    office_location,
    date, 
    date_granularity,
    spend,
    clicks,
    impressions,
    regular_leads,
    purchases,
    video_views,
    workable_leads,
    appointments,
    issues,
    net_sales,
    net_sales_value,
    appointments_value,
    leads
from joined_data
where advertising_channel_type != 'VIDEO'
)

SELECT 
        'Google' AS channel, 
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
        market, 
        CASE WHEN (advertising_channel_type = 'DISCOVERY' OR campaign_name ~* 'demand gen' OR campaign_name ~* 'discovery') THEN 'Demand Gen'
            WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Performance Max'
            WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen' OR campaign_name ~* 'basements keywords' 
                OR campaign_name ~* 'priority markets' OR campaign_name ~* 'worse cpl locations' OR advertising_channel_type = 'SEARCH' THEN 'Search'
        END as campaign_type,
        {{ region_bucket('campaign_name') }} as region_bucket,
        {{ service_type_bucket('ad_group_name') }} as service_type,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        campaign_name as utm_campaign,
        ad_group_name as utm_term,
        NULL as utm_content,
        keyword_text::VARCHAR as utm_keyword,
        keyword_match_type as utm_match_type,
        NULL as utm_placement,
        NULL as utm_discount,
        NULL as utm_lp_variant,
        NULL as utm_msclk_id,
        {{ blended_metrics({
            'spend': 'COALESCE(SUM(spend),0)',
            'clicks': 'COALESCE(SUM(clicks),0)',
            'impressions': 'COALESCE(SUM(impressions),0)',
            'inplatform_leads': 'COALESCE(SUM(regular_leads),0)',
            'video_views': 'COALESCE(SUM(video_views),0)',
            'inplatform_workable_leads': 'COALESCE(SUM(workable_leads),0)',
            'inplatform_appointments': 'COALESCE(SUM(appointments),0)',
            'inplatform_issues': 'COALESCE(SUM(issues),0)',
            'inplatform_net': 'COALESCE(SUM(net_sales_value),0)',
            'inplatform_net_sale_count': 'COALESCE(SUM(net_sales),0)',
            'inplatform_set_value': 'COALESCE(SUM(appointments_value),0)',
            'inplatform_kashurba_leads': 'COALESCE(SUM(leads),0)',
            'inplatform_conversions': 'COALESCE(SUM(purchases),0)'
        }) }}
    FROM (SELECT * FROM final_data)
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
