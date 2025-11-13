{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

WITH office_data as
    (SELECT COUNT(*),
        CASE WHEN office ~* 'R062' THEN 'R062 West Atlanta-GA' ELSE office END as office_adj,
        office_adj as sf_office, 
        case 
            WHEN LEFT(office_adj,1)='R' THEN SPLIT_PART(SPLIT_PART(office_adj,' ',1),'R',2) 
            WHEN LEFT(office_adj,1)='B'THEN SPLIT_PART(office_adj,' ',1)
        end as code, 
        SPLIT_PART(office_adj,' ',2) + SPLIT_PART(office_adj,' ',3) + SPLIT_PART(office_adj,' ',4) as location
    FROM {{ source('gsheet_raw', 'office_locations') }}
    GROUP BY 2,3,4,5
    ORDER BY code ASC)

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name !~* 'warm' THEN 'Campaign Type: Prospecting' 
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
landing_page as landing_page_url,
base_url,
date,
date_granularity,
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, 
sf_office as office_location, 
spend,
impressions,
link_clicks,
purchases,
website_leads+onfacebook_leads as leads,
"offsite_conversion.fb_pixel_custom.Set" as appointment_set    
FROM 
    (SELECT *, SPLIT_PART(landing_page,'/',4) as base_url FROM {{ ref('facebook_performance_by_ad') }} r
    LEFT JOIN {{ source('gsheet_raw','facebook_lp_urls') }} g ON r.ad_name = g.name)
LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_effective_status, 
        case
            --when campaign_name = 'Soc - Meta - Basement - Prospecting - Local - Cold Traffic Sandbox - Lead ABO' and split_part(adset_name,' ',1) = 'Broad' then RIGHT(trim(split_part(adset_name,'-',1)),3)
            when campaign_name = 'Soc - Meta - Roofing - Prospecting - Local - Corpus Christi 0082 Residential Rep - Instant Form' THEN '082'
            when (account_id = '813620678687014' OR account_id = '306770030564777') AND campaign_name !~* 'soc -' then RIGHT(LEFT(campaign_name,4),3) 
            when account_id = '1349056908916556' AND LEFT(campaign_name,4) = 'B071' AND campaign_name !~* 'soc -' THEN 'B001'
            when account_id = '1349056908916556' AND LEFT(campaign_name,4) = 'B078' AND campaign_name !~* 'soc -' THEN 'B002'
            when account_id = '1349056908916556' AND campaign_name !~* 'soc -' THEN LEFT(campaign_name,4)
            when (account_id = '813620678687014' OR account_id = '306770030564777') AND campaign_name ~* 'soc -' then RIGHT(trim(split_part(campaign_name,'-',6)),3) 
            when account_id = '1349056908916556' AND RIGHT(trim(split_part(campaign_name,'-',6)),4) = 'B071' AND campaign_name ~* 'soc -' THEN 'B001'
            when account_id = '1349056908916556' AND RIGHT(trim(split_part(campaign_name,'-',6)),4) = 'B078' AND campaign_name ~* 'soc -' THEN 'B002'
            when account_id = '1349056908916556' AND campaign_name ~* 'soc -' then RIGHT(trim(split_part(campaign_name,'-',6)),4)
        end as code 
        FROM {{ ref('facebook_campaigns') }}) c
        USING(campaign_id, campaign_name, account_id, campaign_effective_status)
    LEFT JOIN office_data USING(code)
