{{ config (
    alias = target.database + '_blended_performance_keywords'
)}}

(SELECT * FROM {{ ref('googleads_sub_sources_google_keywords') }})
UNION ALL
(SELECT * FROM {{ ref('bingads_sub_sources') }} where campaign_type = 'Search')
UNION ALL 
(SELECT * FROM {{ ref('salesforce_sub_sources') }} where (channel = 'Bing' OR channel = 'Google') and campaign_type = 'Search')
