with subsource_cte as (
    select sub_source_id as sf_sub_source_id,sub_source,count(*)
    from {{ source('reporting', 'erie_salesforce_performance') }}
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
        from {{ source('googleads_raw', 'ad_performance_report') }}
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
        FROM {{ source('reporting', 'erie_googleads_campaign_performance') }}
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
        from {{ source('reporting', 'erie_googleads_campaign_performance') }}) t 
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
)

select * from final_data
