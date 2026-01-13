{{ config (
    alias = target.database + '_bingads_landing_page_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
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
    (SELECT date, account_id::varchar as account_id, campaign_id, campaign_name, final_url as landing_page,
        CASE WHEN landing_page ~* 'https://get.eriehome.com/affordable-metal-roofing' THEN 'affordable-metal-roofing_o'
            WHEN landing_page ~* 'nations-number-one-roofing-contractor' THEN 'nations-number-one-roofing-contractor_s'
            WHEN landing_page ~* 'we-need-old-roofs' THEN 'we-need-old-roofs_a'
            WHEN landing_page ~* 'cost-of-roof-replacement' THEN 'cost-of-roof-replacement_h'
            WHEN landing_page ~* 'https://get.eriehome.com/homes-with-old-roofs-wanted' THEN 'homes-with-old-roofs-wanted_e'
            WHEN landing_page ~* 'https://get.eriehome.com/nations-number-one-roofing' THEN 'nations-number-one-roofing_o'
            WHEN landing_page ~* 'https://get.eriehome.com/discover-erie-home-roof-adw' THEN 'discover-erie-home-roof-adw_b'
            WHEN landing_page ~* 'https://get.eriehome.com/attention-homeowners-next-gen-roof' THEN 'attention-homeowners-next-gen-roof_g'
            ELSE 'Other'
        END as lp_variant,
        COALESCE(SUM(impressions::float/2::float),0) AS impressions, 
        COALESCE(SUM(clicks::float/2::float),0) AS clicks, 
        COALESCE(SUM(spend::float/2::float),0) AS spend
    FROM {{ source('bingads_raw', 'destination_url_performance_daily_report') }} 
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT date, account_id::varchar as account_id, campaign_id, campaign_name, final_url as landing_page,
        CASE WHEN landing_page ~* 'https://get.eriehome.com/affordable-metal-roofing' THEN 'affordable-metal-roofing_q'
            WHEN landing_page ~* 'nations-number-one-roofing-contractor' THEN 'nations-number-one-roofing-contractor_r'
            WHEN landing_page ~* 'we-need-old-roofs' THEN 'we-need-old-roofs_i'
            WHEN landing_page ~* 'cost-of-roof-replacement' THEN 'cost-of-roof-replacement_j'
            WHEN landing_page ~* 'https://get.eriehome.com/homes-with-old-roofs-wanted' THEN 'homes-with-old-roofs-wanted_i'
            WHEN landing_page ~* 'https://get.eriehome.com/nations-number-one-roofing' THEN 'nations-number-one-roofing_n'
            WHEN landing_page ~* 'https://get.eriehome.com/discover-erie-home-roof-adw' THEN 'discover-erie-home-roof-adw_c'
            WHEN landing_page ~* 'https://get.eriehome.com/attention-homeowners-next-gen-roof' THEN 'attention-homeowners-next-gen-roof_h'
            ELSE 'Other'
        END as lp_variant,
        COALESCE(SUM(impressions::float/2::float),0) AS impressions, 
        COALESCE(SUM(clicks::float/2::float),0) AS clicks, 
        COALESCE(SUM(spend::float/2::float),0) AS spend
    FROM {{ source('bingads_raw', 'destination_url_performance_daily_report') }} 
    GROUP BY 1,2,3,4,5,6),
    
initial_data as 
    (SELECT account_id, campaign_id, campaign_name, landing_page, lp_variant, impressions, clicks, spend, 
        {{ get_date_parts('date') }}
    FROM 
        (SELECT * FROM lp_data
        WHERE lp_variant != 'Other')
    ),
  
final_data as
    ({%- for date_granularity in date_granularity_list %}
    SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        account_id,
        campaign_id,
        campaign_name,
        RIGHT(LEFT(campaign_name,4),3) as code,
        landing_page,
        lp_variant,
        COALESCE(SUM(spend),0) as spend,
        COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(clicks),0) as clicks 
    FROM initial_data
    GROUP BY 1,2,3,4,5,6,7,8
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
WHERE landing_page !~* 'regionalization'
