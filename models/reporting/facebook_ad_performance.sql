{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'cold' THEN 'Campaign Type: Prospecting' 
    WHEN campaign_name ~* 'warm' THEN 'Campaign Type: Retargeting' 
    WHEN campaign_name ~* 'LP Clicks Traffic' THEN 'Campaign Type: Traffic' 
    WHEN campaign_name ~* 'LP Views Leads' THEN 'Campaign Type: View Content' 
END as campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
website_leads+onfacebook_leads as leads
FROM {{ ref('facebook_performance_by_ad') }}
