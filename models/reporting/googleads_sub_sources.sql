{{ config (
    materialized = 'ephemeral'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

{#
    Google Ads and YouTube, previously googleads_sub_sources_google and
    googleads_sub_sources_youtube.

    The two models ran the same query over the same sources and differed only in
    which advertising_channel_type they kept: VIDEO is YouTube, everything else
    is Google. They are one model now, and the channel split is presentation
    logic in the final SELECT.

    Two grains feed it:
      - ad grain, for everything except Performance Max
      - asset group grain, for Performance Max (which has no ad-level rows)

    Metrics come from raw_ad_daily rather than googleads_ad_performance because
    the two agree exactly on spend/clicks/impressions (verified across every
    channel type) AND the raw grain splits an ad's metrics across its final
    URLs. 101 ads carry more than one final URL and 15% of all spend, so that
    split is load bearing: taking metrics from the ad grain instead would
    double count them.
#}

WITH subsource_cte as (
    select sub_source_id as sf_sub_source_id, sub_source, count(*)
    from {{ source('reporting','salesforce_performance') }}
    group by 1,2
),

{#  Raw ad performance, restated at every date granularity and split by final
    URL. This is the expensive step, and it now runs once instead of twice. #}
raw_ad_daily as (
    {%- for date_granularity in date_granularity_list %}
    select  ad_final_urls,
            ad_id,
            ad_group_id,
            campaign_id,
            '{{date_granularity}}' as date_granularity,
            {{date_granularity}} as date,
            advertising_channel_type,
            sum(cost_micros::FLOAT/1000000::FLOAT) as spends,
            sum(clicks::FLOAT) as click,
            sum(impressions::FLOAT) as impression
    from (select *, {{ get_date_parts('date') }} from {{ source('googleads_raw', 'ad_performance_report') }})
    left join {{ ref('googleads_campaign_types') }} using(campaign_id)
    group by 1,2,3,4,5,6,7
    {% if not loop.last %}UNION ALL
    {% endif %}
    {%- endfor %}
),

ad_grain as (
    SELECT  ad_final_urls,
            u.sub_source_id,
            sub_source,
            ad_id::VARCHAR as ad_id,
            ad_name,
            ad_group_id::VARCHAR as ad_group_id,
            ad_group_name,
            campaign_name,
            campaign_id::VARCHAR as campaign_id,
            campaign_type_default,
            campaign_status,
            advertising_channel_type,
            date,
            date_granularity,
            erie_type,
            market,
            office,
            office_location,
            account_id::VARCHAR as account_id,
            spends as spend,
            click as clicks,
            impression as impressions,
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
    FROM {{ source('reporting','googleads_ad_performance') }}
    LEFT JOIN raw_ad_daily using(ad_id, ad_group_id, campaign_id, date, date_granularity)
    LEFT JOIN {{ ref('googleads_ad_url_subsources') }} u using(ad_final_urls)
    LEFT JOIN subsource_cte on subsource_cte.sf_sub_source_id::varchar = u.sub_source_id::varchar
    WHERE date >= '2022-12-01'
    and advertising_channel_type != 'PERFORMANCE_MAX'
),

asset_group_grain as (
    SELECT  '(not set)' as ad_final_urls,
            t.sub_source_id,
            sub_source,
            NULL as ad_id,
            NULL as ad_name,
            ad_group_id::VARCHAR as ad_group_id,
            ad_group_name,
            campaign_name,
            campaign_id::VARCHAR as campaign_id,
            campaign_type_default,
            campaign_status,
            advertising_channel_type,
            date,
            date_granularity,
            erie_type,
            market,
            office,
            office_location,
            account_id::VARCHAR as account_id,
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
    FROM (select *,
            case
                when campaign_name !~* '0000' then right(split_part(campaign_name,' Warm',1),3)
                else '797'
            end as sub_source_id
            from {{ source('reporting','googleads_asset_group_performance') }}) t
    LEFT JOIN {{ ref('googleads_campaign_types') }} using(campaign_id)
    LEFT JOIN subsource_cte on subsource_cte.sf_sub_source_id::varchar = t.sub_source_id::varchar
    WHERE date >= '2022-12-01'
    and advertising_channel_type != 'VIDEO'
),

final_data as (
    SELECT * FROM ad_grain
    UNION ALL
    SELECT * FROM asset_group_grain
)

SELECT
        CASE WHEN advertising_channel_type = 'VIDEO' THEN 'YouTube' ELSE 'Google' END AS channel,
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
        CASE
            WHEN advertising_channel_type = 'VIDEO' THEN
                CASE WHEN campaign_name ~* 'cold' THEN 'Prospecting'
                    WHEN campaign_name ~* 'warm' THEN 'Retargeting'
                END
            WHEN (advertising_channel_type = 'DISCOVERY' OR campaign_name ~* 'demand gen' OR campaign_name ~* 'discovery') THEN 'Demand Gen'
            WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Performance Max'
            WHEN campaign_name ~* 'Branded' OR campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen' OR campaign_name ~* 'basements keywords'
                OR campaign_name ~* 'priority markets' OR campaign_name ~* 'worse cpl locations' OR advertising_channel_type = 'SEARCH' THEN 'Search'
        END as campaign_type,
        {{ region_bucket('campaign_name') }} as region_bucket,
        CASE WHEN advertising_channel_type = 'VIDEO' THEN NULL
            ELSE {{ service_type_bucket('ad_group_name') }}
        END as service_type,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        campaign_name as utm_campaign,
        CASE WHEN advertising_channel_type = 'VIDEO' THEN ad_group_id::VARCHAR
            ELSE ad_group_name
        END as utm_term,
        CASE WHEN advertising_channel_type = 'VIDEO' THEN TRIM(REPLACE(LOWER(ad_name),' - ',' '))::VARCHAR
            ELSE ad_id::VARCHAR
        END as utm_content,
        NULL as utm_keyword,
        NULL as utm_match_type,
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
    FROM final_data
    WHERE ((sub_source !~* 'CallRail' and sub_source !~* 'Link Extension') or sub_source is null or sub_source = '')
    and date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
