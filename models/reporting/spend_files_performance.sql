{{ config (
    alias = target.database + '_spend_files_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
WITH filtered_data as
    (SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('s3_raw','spend_files') }}
    WHERE _file IN (SELECT MAX(_file) FROM {{ source('s3_raw','spend_files') }})),

    
    final_data as 
    ({%- for date_granularity in date_granularity_list %}
    SELECT  
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        office_name, ad_source,
        COALESCE(SUM(spend),0) as spend,
        COALESCE(SUM(sum_impressions_),0) as impressions,
        COALESCE(SUM(sum_clicks_),0) as clicks,
        COALESCE(SUM(leads),0) as leads,
        COALESCE(SUM(sum_sets_),0) as appointments,
        COALESCE(SUM(sum_hits_),0) as hits,
        COALESCE(SUM(sum_issues_),0) as issues,
        COALESCE(SUM(sum_net_sales_),0) as net,
        COALESCE(SUM(sum_workable_leads_),0) as workable_leads
        FROM filtered_data
        GROUP BY 1,2,3,4
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %})
    

SELECT 
    date,
    date_granularity,
    office_name, 
    ad_source,
    spend,
    impressions,
    clicks,
    leads,
    appointments,
    hits,
    issues,
    net,
    workable_leads
FROM final_data