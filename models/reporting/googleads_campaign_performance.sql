{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Discovery' THEN 'Campaign Type: Discovery'
    WHEN campaign_name ~* 'Performance Max' OR campaign_name ~* 'PMAX' THEN 'Campaign Type: Performance Max'
    WHEN campaign_name ~* 'Branded' THEN 'Campaign Type: Branded'
    WHEN campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen keywords' or campaign_name ~* 'basements keywords' THEN 'Campaign Type: Non Branded'
    WHEN campaign_name ~* 'youtube' THEN 'Campaign Type: Youtube'
    ELSE 'Campaign Type: Other'
END as campaign_type_default,
date,
date_granularity,
CASE WHEN account_id  = '4560674777' THEN 'Roofing'
    WHEN account_id = '2819798401' THEN 'Basement'
    END AS erie_type,
CASE WHEN campaign_name ~* 'all areas' THEN 'National'
    WHEN campaign_name ~* 'warm' THEN 'Retargeting'
    WHEN campaign_name !~* 'all areas' OR campaign_name !~* 'warm' THEN 'Local'
END as market,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
kashurbagetpricing as leads,
video_views
FROM {{ ref ('googleads_performance_by_campaign') }}
