{{ config (
    alias = target.database + '_spend_files_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
WITH filtered_data as
    (SELECT *
    FROM {{ source('s3_raw','spend_files') }}
    WHERE _file IN (SELECT MAX(_file) FROM {{ source('s3_raw','spend_files') }})),

    
    final_data as 
    ({%- for date_granularity in date_granularity_list %}
    SELECT  
        '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}', TO_TIMESTAMP(date, 'MM/DD/YYYY HH:MI:SS AM')) as date,
        office_name, ad_source,
        COALESCE(SUM(spend),0) as spend,
        COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(clicks),0) as clicks,
        COALESCE(SUM(leads),0) as leads,
        COALESCE(SUM(sets),0) as appointments,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issues),0) as issues,
        COALESCE(SUM(netsales),0) as net,
        COALESCE(SUM(workable_leads),0) as workable_leads
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
