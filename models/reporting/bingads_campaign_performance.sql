{{ config (
    alias = target.database + '_bingads_campaign_performance'
)}}

WITH office_data as
    (SELECT office as sf_office, SPLIT_PART(SPLIT_PART(office,' ',1),'R',2) as code, SPLIT_PART(office,' ',2) as location
    FROM {{ source ('gsheet_raw', 'office_locations') }}
    GROUP BY office
    ORDER BY code ASC)

SELECT 
account_id,
c.campaign_name,
campaign_id,
c.campaign_status,
CASE WHEN c.campaign_name ~* 'Branded' THEN 'Campaign Type: Branded'
    WHEN c.campaign_name ~* 'Metal Roofing Keywords' THEN 'Campaign Type: Non Branded'
    ELSE 'Campaign Type: Other'
END as campaign_type_default,
date,
date_granularity,
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office,
sf_office as office_location,
spend,
impressions,
clicks,
conversions as leads,
revenue as revenue
FROM {{ ref('bingads_performance_by_campaign') }}
LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status, RIGHT(LEFT(campaign_name,4),3) as code 
            FROM {{ ref('bingads_campaigns') }}) c 
    USING(campaign_id, account_id)
LEFT JOIN office_data USING(code)
