{{ config (
    alias = target.database + '_googleads_sub_sources_youtube_test'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
WITH subsource_cte as (
    select sub_source_id as sf_sub_source_id,sub_source,count(*)
    from {{ source('reporting','salesforce_performance') }}
    group by 1,2
    ),

 subsource_id_cte as (
        
        select  
                ad_final_urls,{{ get_date_parts('date') }}, 
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
        group by 1,2,3,4,5,6,7
        
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
 AND campaign_max_updated_date.max_updated_at = campaign_history.updated_at),

joined_data as  ( (  
    
        SELECT  ad_final_urls,
                sub_source_id, 
                ad_id::VARCHAR,
                ad_name,
                ad_group_id::VARCHAR,
                campaign_name,
                date, 
                campaign_id,
                campaign_type_default,
                advertising_channel_type,
                date_granularity, 
                erie_type, 
                market, 
                sub_source,
                office,
                office_location,
                spends as spend, 
                clicks, 
                impressions,
                leads, 
                video_views,
                account_id, 
                campaign_status
        FROM {{ source('reporting','googleads_ad_performance') }}
        left join (
            {%- for date_granularity in date_granularity_list %}
            select  ad_final_urls,
                    ad_id,
                    ad_group_id,
                    campaign_id,
                    '{{date_granularity}}' as date_granularity,
                    {{date_granularity}} as date,
                    sum(cost_micros::FLOAT/1000000::FLOAT) as spends
                    from {{ source('googleads_raw', 'ad_performance_report') }}
                    left join campaign_types
                    USING(campaign_id)
                    group by 1,2,3,4,5,6
                    {% if not loop.last %}UNION ALL
                    {% endif %}
                {% endfor %}
                    ) 
                    using(ad_id, ad_group_id, campaign_id, date, date_granularity)
        left join subsource_id_cte using(ad_final_urls)
        left join subsource_cte on subsource_cte.sf_sub_source_id::varchar = subsource_id_cte.sub_source_id::varchar
        where date >= '2022-12-01'
        and advertising_channel_type != 'PERFORMANCE_MAX'
        order by date, sub_source_id, campaign_name,ad_final_urls
        
        )
        
        Union all 
        
        (SELECT  '(not set)' as ad_final_urls,
                NULL as ad_id,
                NULL as ad_name,
                NULL as ad_group_id,
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
                office,
                office_location,
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
        from {{ source('reporting','googleads_campaign_performance') }})  
        left join campaign_types USING(campaign_id)
        left join subsource_cte on subsource_cte.sf_sub_source_id::varchar = t.sub_source_id::varchar
        where date >= '2022-12-01'
        and advertising_channel_type = 'PERFORMANCE_MAX'
        order by date, sub_source_id, campaign_name )),
        
    
final_data as (
select 
    account_id, 
    ad_id,
    ad_name,
    ad_group_id,
    campaign_name, 
    campaign_id, 
    campaign_type_default,
    campaign_status,
    advertising_channel_type,
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
where ((sub_source !~* 'CallRail' and sub_source !~* 'Link Extension') or sub_source is null or sub_source = '')
and advertising_channel_type = 'VIDEO'
)

SELECT 
        'YouTube' AS channel, 
        date, 
        date_granularity, 
        office, 
        office_location,  
        NULL as sf_locations, 
        NULL as source,
        sub_source_id, 
        sub_source,
        NULL as zip, erie_type, 
        market,
        CASE WHEN campaign_name ~* 'cold' THEN 'Prospecting' 
            WHEN campaign_name ~* 'warm' THEN 'Retargeting'
        END as campaign_type,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        campaign_id::VARCHAR as utm_campaign,
        ad_group_id::VARCHAR as utm_term,
        TRIM(REPLACE(LOWER(ad_name),' - ',' '))::VARCHAR as utm_content,
        NULL as utm_keyword,
        NULL as utm_match_type,
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
