{{ config (
    alias = target.database + '_bingads_landing_page_performance'
)}}

WITH office_data as
    (SELECT COUNT(*),
        CASE WHEN office ~* 'R062' THEN 'R062 West Atlanta-GA' ELSE office END as office_adj,
        office_adj as sf_office,
        SPLIT_PART(SPLIT_PART(office_adj,' ',1),'R',2) as code, 
        SPLIT_PART(office_adj,' ',2) as location
    FROM {{ source('gsheet_raw', 'office_locations') }}
    GROUP BY 2,3,4,5
    ORDER BY code ASC),

lp_data as 
    (SELECT account_id::varchar as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name, final_url as landing_page,
      impressions, clicks, spend, 
      {{ get_date_parts('date') }}
    FROM {{ source('bingads_raw', 'destination_url_performance_daily_report') }})
  
final_data as
    ({%- for date_granularity in date_granularity_list %}
    SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        account_id,
        ad_group_id, 
        ad_group_name,
        campaign_id,
        campaign_name,
        RIGHT(LEFT(campaign_name,4),3) as code,
        landing_page,
        CASE WHEN landing_page ~* 'https://get.eriehome.com/affordable-metal-roofing/' THEN 'affordable-metal-roofing_i'
            WHEN landing_page ~* 'nations-number-one-roofing-contractor' THEN 'nations-number-one-roofing-contractor_l'
            WHEN landing_page ~* 'https://get.eriehome.com/nations-number-one-roofing/' THEN 'nations-number-one-roofing_e'
            WHEN landing_page ~* 'https://get.eriehome.com/nations-number-one-roofing/' THEN 'we-need-old-roofs_a'
            ELSE 'Other'
        END as lp_variant,
        COALESCE(SUM(spend),0) as spend,
        COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(clicks),0) as clicks 
    FROM lp_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %})

SELECT 
account_id,
campaign_id,
campaign_name,
CASE WHEN campaign_name ~* 'Branded' THEN 'Campaign Type: Branded'
    WHEN campaign_name ~* 'Metal Roofing Keywords' THEN 'Campaign Type: Non Branded'
    ELSE 'Campaign Type: Other'
END as campaign_type_default,
ad_group_id,
ad_group_name,
landing_page,
lp_variant,
date,
date_granularity,
CASE WHEN account_id = '149506166' THEN 'Basement'
    WHEN account_id = '149034657' THEN 'Roofing'
    END AS erie_type,
'National' as market,
CASE WHEN location IS NULL THEN 'Unknown' ELSE location END as office, 
sf_office as office_location, 
spend,
impressions,
clicks  
FROM final_data
LEFT JOIN office_data USING(code)
