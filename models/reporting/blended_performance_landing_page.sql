{{ config (
    alias = target.database + '_blended_performance_landing_page'
)}}

(SELECT * FROM {{ ref('googleads_sub_sources_landing_page') }})
UNION ALL
(SELECT * FROM {{ ref('bingads_sub_sources_landing_page') }})
UNION ALL 
(SELECT * FROM {{ ref('salesforce_sub_sources_landing_page') }} 
WHERE (channel = 'Bing' OR channel = 'Google') )
