{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

WITH office_data as
    (SELECT office as sf_office, 
        case 
            WHEN LEFT(office,1)='R' THEN SPLIT_PART(SPLIT_PART(office,' ',1),'R',2) 
            WHEN LEFT(office,1)='B'THEN SPLIT_PART(office,' ',1)
        end as code, 
        SPLIT_PART(office,' ',2) + SPLIT_PART(office,' ',3) + SPLIT_PART(office,' ',4) as location
    FROM {{ source('gsheet_raw', 'office_locations') }}
    GROUP BY office
    ORDER BY code ASC)
  
SELECT 
account_id,
ad_name,
ad_id,
ad_status,
ad_group_name,
ad_group_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN advertising_channel_type = 'DISCOVERY' THEN 'Campaign Type: Discovery'
    WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Campaign Type: Performance Max'
    WHEN advertising_channel_type = 'VIDEO' THEN 'Campaign Type: Youtube'
    WHEN campaign_name ~* 'Branded' THEN 'Campaign Type: Branded'
    WHEN campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen keywords' or campaign_name ~* 'basements keywords' THEN 'Campaign Type: Non Branded'
    ELSE 'Campaign Type: Other'
END as campaign_type_default,
date,
date_granularity,
CASE WHEN account_id  = '4560674777' THEN 'Roofing'
    WHEN account_id = '2819798401' THEN 'Basement'
    END AS erie_type,
CASE WHEN campaign_name ~* 'All Areas' THEN 'National'
    ELSE 'Local'
    END as market,
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, 
sf_office as office_location, 
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
kashurbagetpricing as leads,
video_views
FROM {{ ref('googleads_performance_by_ad') }}
LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status, advertising_channel_type,  
            case 
                when account_id = '4560674777' THEN RIGHT(LEFT(campaign_name,4),3) 
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0071' THEN 'B001'
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0078' THEN 'B002'
                when account_id = '2819798401' THEN LEFT(campaign_name,4)
            end as code 
                FROM {{ ref('googleads_campaigns') }}) USING(campaign_id, campaign_name, account_id, campaign_status)
    LEFT JOIN office_data USING(code)
