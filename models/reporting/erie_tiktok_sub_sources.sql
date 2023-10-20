with source_cte as (select sub_source_id, sub_source
from {{ source('reporting', 'erie_salesforce_performance') }}
group by 1,2) 

, url_cte as (
select ad_id,landing_page_url, campaign_name, campaign_id, date,
        case
            when split_part(landing_page_url,'&',2) ~* 'slc' then split_part(split_part(landing_page_url,'&',2),'=',2)
            else right(split_part(landing_page_url,'/',4),3)
        end as sub_source_id, 
        sum(cost) as spends

from {{ source('supermetrics_raw', 'tik_ads_insights') }}
WHERE date >= '2023-05-01'
group by 1,2,3,4,5,6
)
, source as (
select ad_id, 
date_trunc('day',date) as date,
'day' as date_granularity,
landing_page_url, url_cte.sub_source_id, sub_source, sum(spends) as spends
from url_cte
left join source_cte on source_cte.sub_source_id = url_cte.sub_source_id
group by 1,2,3,4,5,6
union all 
select ad_id, 
date_trunc('week',date) as date,
'week' as date_granularity,
landing_page_url, url_cte.sub_source_id, sub_source, sum(spends) as spends
from url_cte
left join source_cte on source_cte.sub_source_id = url_cte.sub_source_id
group by 1,2,3,4,5,6
union all
select ad_id, 
date_trunc('month',date) as date,
'month' as date_granularity,
landing_page_url, url_cte.sub_source_id, sub_source, sum(spends) as spends
from url_cte
left join source_cte on source_cte.sub_source_id = url_cte.sub_source_id
group by 1,2,3,4,5,6
union all
select ad_id, 
date_trunc('quarter',date) as date,
'quarter' as date_granularity,
landing_page_url, url_cte.sub_source_id, sub_source, sum(spends) as spends
from url_cte
left join source_cte on source_cte.sub_source_id = url_cte.sub_source_id
group by 1,2,3,4,5,6
union all
select ad_id, 
date_trunc('year',date) as date,
'year' as date_granularity,
landing_page_url, url_cte.sub_source_id, sub_source, sum(spends) as spends
from url_cte
left join source_cte on source_cte.sub_source_id = url_cte.sub_source_id
group by 1,2,3,4,5,6
)

, joined_data as (
SELECT 'TikTok' AS channel, 
        date, 
        date_granularity, 
        NULL::VARCHAR(256) as office, 
        NULL::VARCHAR(256) as office_location, 
        NULL::VARCHAR(256) as sf_locations, 
        sub_source_id, 
        sub_source, 
        NULL::VARCHAR(256) as zip, 
        'Roofing' as erie_type,
        CASE WHEN campaign_name ~* 'National' THEN 'National'
            --WHEN campaign_name ~* 'Consolidation' THEN 'Consolidation'
            WHEN campaign_name !~* 'National' THEN 'Local'
        END as market,
        CASE WHEN campaign_name ~* 'Prospecting' THEN 'Prospecting' 
            WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting'
        END as campaign_type,
        campaign_name,
        NULL::VARCHAR(256) as dispo,
        NULL::VARCHAR(256) as call_disposition,
        NULL::VARCHAR(256) as status_detail,
        COALESCE(SUM(spends),0) AS spend,
        COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(impressions),0) AS impressions,
        COALESCE(SUM(submit_form),0) AS inplatform_leads,
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
    FROM {{ source('reporting', 'erie_tiktok_ad_performance') }}
    LEFT JOIN source using(ad_id, date, date_granularity)
    WHERE date >= '2023-05-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    
select * from joined_data
