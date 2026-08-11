{{ config (
    alias = target.database + '_blended_performance'
)}}


(SELECT * FROM {{ ref('googleads_sub_sources') }})
UNION ALL
(SELECT * FROM {{ ref('bingads_sub_sources') }})
UNION ALL
(SELECT * FROM {{ ref('salesforce_sub_sources') }})
UNION ALL
(SELECT * FROM {{ ref('nextdoor_sub_sources') }})
