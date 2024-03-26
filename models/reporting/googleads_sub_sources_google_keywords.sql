{{ config (
    alias = target.database + '_googleads_sub_sources_google_keywords'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
WITH campaign_max_updated_date as (
 SELECT id , max(updated_at) as max_updated_at
 from {{ source('googleads_raw', 'campaign_history') }}
 group by 1),

campaign_types as (
 SELECT campaign_max_updated_date.id as campaign_id, advertising_channel_type
 FROM campaign_max_updated_date 
 LEFT JOIN {{ source('googleads_raw', 'campaign_history') }}
 ON campaign_max_updated_date.id = campaign_history.id 
 AND campaign_max_updated_date.max_updated_at = campaign_history.updated_at),

joined_data as  (
    
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
                leads, 
                video_views,
                account_id, 
                campaign_status
        FROM {{ source('reporting','googleads_keyword_performance') }}
        left join campaign_types USING(campaign_id)
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
    leads,
    video_views
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
        CASE WHEN advertising_channel_type = 'DISCOVERY' THEN 'Discovery'
            WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Performance Max'
            WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen' OR campaign_name ~* 'basements keywords' 
                OR campaign_name ~* 'priority markets' OR campaign_name ~* 'worse cpl locations' OR advertising_channel_type = 'SEARCH' THEN 'Search'
        END as campaign_type,
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
        COALESCE(SUM(spend),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(leads),0) AS inplatform_leads,
        COALESCE(SUM(video_views),0) as video_views,
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
    FROM (SELECT * FROM final_data)
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
