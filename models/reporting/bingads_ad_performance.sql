{{ config (
    alias = target.database + '_bingads_ad_performance'
)}}

WITH office_data as
    (SELECT office as sf_office, SPLIT_PART(SPLIT_PART(office,' ',1),'R',2) as code, SPLIT_PART(office,' ',2) as location
    FROM {{ source('gsheet_raw', 'office_locations') }}
    GROUP BY office
    ORDER BY code ASC)

SELECT 
account_id,
ad_id,
ad_group_name,
ad_status,
ad_group_id,
ad_group_status,
c.campaign_name,
campaign_id,
c.campaign_status,
date,
date_granularity,
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office,
sf_office as office_location,
CASE WHEN account_id = '149506166' THEN 'Basement'
    WHEN account_id = '149034657' THEN 'Roofing'
END as erie_type,
spend,
impressions,
clicks,
conversions as leads,
revenue as revenue
FROM {{ ref('bingads_performance_by_ad') }}
LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status, RIGHT(LEFT(campaign_name,4),3) as code 
          FROM {{ ref('bingads_campaigns') }}) c 
    USING(campaign_id, account_id)
LEFT JOIN office_data USING(code)
