{{ config (
    alias = target.database + '_googleads_keyword_performance'
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
keyword_text,
keyword_id,
keyword_status,
keyword_match_type,
ad_group_name,
ad_group_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'discovery' THEN 'Campaign Type: Discovery'
    WHEN campaign_name ~* 'demand gen' THEN 'Campaign Type: Demand Gen'
    WHEN advertising_channel_type = 'PERFORMANCE_MAX' THEN 'Campaign Type: Performance Max'
    WHEN advertising_channel_type = 'VIDEO' THEN 'Campaign Type: Youtube'
    WHEN campaign_name ~* 'Branded' THEN 'Campaign Type: Branded'
    WHEN campaign_name ~* 'metal roofing keywords' OR campaign_name ~* 'NBS evergreen' or campaign_name ~* 'basements keywords' or campaign_name ~* 'priority markets'
        or campaign_name ~* 'worse cpl locations' THEN 'Campaign Type: Non Branded'
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
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, 
sf_office as office_location, 
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
kashurbagetpricing as leads,
video_views
FROM {{ ref('googleads_performance_by_keyword') }}
LEFT JOIN (SELECT campaign_id, campaign_name, account_id, campaign_status,  
            case 
                when account_id = '4560674777' THEN RIGHT(LEFT(campaign_name,4),3) 
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0071' THEN 'B001'
                when account_id = '2819798401' AND LEFT(campaign_name,4) = '0078' THEN 'B002'
                when account_id = '2819798401' THEN LEFT(campaign_name,4)
            end as code 
                FROM {{ ref('googleads_campaigns') }}) USING(campaign_id, campaign_name, account_id, campaign_status)
    LEFT JOIN office_data USING(code)
