{{ config (
    alias = target.database + '_googleads_sub_sources_google'
)}}

WITH subsource_cte as (
    select sub_source_id as sf_sub_source_id,sub_source,count(*)
    from {{ ref('salesforce_performance') }}
    group by 1,2
    ),

 subsource_id_cte as (
        
        select  
                ad_final_urls,
                case
                    when RIGHT(ad_final_urls, 5) = 'tep/]' then LEFT(RIGHT(ad_final_urls, 16),3)
                        when (ad_final_urls = '[http://go.eriemetalroofs.com/erie-youtube-metal-roofing-f/]' 
                            or ad_final_urls = '[https://go.eriemetalroofs.com/erie-nn/]' 
                            or  ad_final_urls = '[https://go.eriemetalroofs.com/erie-youtube-metal-roofing-f/]'  
                            or ad_final_urls ~*'https://www.eriehome.com/lp/metal-roofing/?utm_source=r-dgd') 
                        then 'other'
                        else RIGHT(ad_final_urls, 5)
                end as test,
                case 
                    when test ~* '/]' then left(test,3)
                    when test ~* ']' then left(test,4)
                    else test
                end as test2, 
                case 
                    when test2 ~* '=' and test2 !~* '=48' then right(test2,3)
                    when test2 ~* '=' and test2 ~* '=48' then right(test2,2)
                    when test2 ~* '-' then right(test2,2) 
                    else test2
                end as sub_source_id,
                sum(cost_micros::FLOAT)
        from {{ source('googleads_raw','ad_performance_report') }}
        group by 1,2
        
    ),

campaign_max_updated_date as (
 SELECT id , max(updated_at) as max_updated_at
 from {{ source('googleads_raw', 'campaign_history') }}
 group by 1
),

campaign_types as (
 SELECT campaign_max_updated_date.id as campaign_id, advertising_channel_type
 FROM campaign_max_updated_date 
 LEFT JOIN {{ source('googleads_raw', 'campaign_history') }}
 ON campaign_max_updated_date.id = campaign_history.id 
 AND campaign_max_updated_date.max_updated_at = campaign_history.updated_at
),

joined_data as  ( (  
    
    
        SELECT  ad_final_urls,
                sub_source_id, 
                campaign_name,
                date, 
                campaign_id,
                campaign_type_default,
                advertising_channel_type,
                date_granularity, 
                erie_type, 
                market, 
                sub_source, 
                spends as spend, 
                clicks, 
                impressions,
                leads, 
                video_views,
                account_id, 
                campaign_status
        FROM {{ ref('googleads_campaign_performance') }}
        left join (
            
            select  ad_final_urls,
                    advertising_channel_type,
                    campaign_id,
                    date_trunc('day', date) as date, 
                    'day' as date_granularity,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5
                    
            Union all 
            
            select  ad_final_urls, 
                    advertising_channel_type,
                    campaign_id,
                    date_trunc('week', date) as date, 
                    'week' as date_granularity,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5
            
            Union all
            
            select  ad_final_urls, 
                    advertising_channel_type,
                    campaign_id,
                    date_trunc('month', date) as date, 
                    'month' as date_granularity,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5
                    
            Union all
            
            select  ad_final_urls, 
                    advertising_channel_type,
                    campaign_id,
                    date_trunc('quarter', date) as date, 
                    'quarter' as date_granularity,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5
                    
            Union all
            
            select  ad_final_urls, 
                    advertising_channel_type,
                    campaign_id,
                    date_trunc('year', date) as date, 
                    'year' as date_granularity,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5
                    
                    ) 
                    using(campaign_id,date, date_granularity)
        left join subsource_id_cte using(ad_final_urls)
        left join subsource_cte on subsource_cte.sf_sub_source_id::varchar = subsource_id_cte.sub_source_id::varchar
        where date between '2023-05-01' and current_date
        and advertising_channel_type != 'PERFORMANCE_MAX'
        order by date, sub_source_id, campaign_name,ad_final_urls
        
        )
        
        Union all 
        
        (SELECT  '(not set)' as ad_final_urls,
                sub_source_id, 
                campaign_name,
                date, 
                campaign_id,
                campaign_type_default,
                advertising_channel_type,
                date_granularity, 
                erie_type, 
                market, 
                sub_source, 
                spend, 
                clicks, 
                impressions,
                leads, 
                video_views,
                account_id, 
                campaign_status
        FROM 
        (select *,
        case 
            when campaign_name !~* '0000' then right(split_part(campaign_name,' Warm',1),3)
            else '797'
        end as sub_source_id
        from {{ ref('googleads_campaign_performance') }}) t 
        left join campaign_types USING(campaign_id)
        left join subsource_cte on subsource_cte.sf_sub_source_id::varchar = t.sub_source_id::varchar
        where date between '2023-05-01' and current_date
        and advertising_channel_type = 'PERFORMANCE_MAX'
        order by date, sub_source_id, campaign_name )),
        
    
final_data as (
select 
    account_id, 
    campaign_name, 
    campaign_id, 
    campaign_type_default,
    campaign_status,
    sub_source_id, 
    sub_source, 
    erie_type, 
    market, 
    date, 
    date_granularity,
    spend,
    clicks,
    impressions,
    leads,
    video_views
from joined_data
where ((sub_source !~* 'CallRail' and sub_source !~* 'Link Extension') or sub_source is null or sub_source = '')
and advertising_channel_type != 'VIDEO'
)

SELECT 
        'Google' AS channel, 
        date, 
        date_granularity, 
        CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, 
        sf_office as office_location, 
        NULL as sf_locations, 
        sub_source_id, 
        sub_source,
        NULL as zip, 
        erie_type, 
        market, 
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
    FROM (SELECT * FROM final_data WHERE campaign_type_default != 'Campaign Type: Youtube')
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
