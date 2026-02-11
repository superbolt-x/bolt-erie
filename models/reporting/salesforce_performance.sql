{{ config (
    alias = target.database + '_salesforce_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
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
    ORDER BY code ASC),
    
    filetered_data as
    (SELECT *, {{ get_date_parts('lead_entry_date') }}
    FROM {{ source('snowflake_superbolt','superbolt_daily_file') }}
    WHERE _fivetran_deleted IS false),

    
    final_data as 
    ({%- for date_granularity in date_granularity_list %}
    SELECT  
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        CASE WHEN market ~* 'R062' THEN 'R062 West Atlanta-GA' ELSE market END as market_adj, 
        state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, 
        CASE WHEN utm_source ~* 'facebook' AND utm_campaign::varchar ~* 'Adv\\+' THEN TRIM(REPLACE(REPLACE(REPLACE(utm_campaign,'%28','('),'%29',')'),'%3A',':'))
            WHEN utm_source ~* 'facebook' AND utm_campaign::varchar !~* 'Adv\\+' THEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(utm_campaign,'%28','('),'%29',')'),'%3A',':'),'+',' '),'%2B','+'))
            WHEN utm_source = 'nextdoor' OR source IN ('SM5','BSM5') THEN REPLACE(utm_campaign,'_',' ')
            ELSE utm_campaign
        END as utm_campaign_adj, 
        utm_term, 
        CASE WHEN utm_content ~* 'shorts_stay_off_the_ladder_gutter_guard_4000_value_banner_split_gg_lp' THEN 'shorts stay off the ladder gutter guards 4000 value banner split gg lp'
            WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(lower(utm_content),'lps','lp'),'__',' '),'_',' '),' - ',' '))::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount, utm_lp_variant,utm_campaign_id,
        COUNT(DISTINCT lead_id) as leads,
        SUM(COALESCE(number_of_calls,0)) as calls,
        SUM(COALESCE("set",0)) as appointments,
        SUM(COALESCE(demo,0)) as demos,
        SUM(COALESCE(hits,0)) as hits,
        SUM(COALESCE(issued,0)) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        SUM(COALESCE(sold,0)) as closed_deals,
        SUM(COALESCE("gross__",0)) as gross,
        SUM(COALESCE("net__",0)) as net,
        SUM(COALESCE(workable_leads,0)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads,
        SUM(COALESCE(net_sale_count,0)) as net_sale_count,
        SUM(COALESCE(median_value_per_set::float,0)) as set_value
        FROM filetered_data
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %})
    

SELECT 
    date,
    date_granularity,
    market_adj as market,
    state, source,zip, sub_source_id, sub_source, dispo, call_disposition, status_detail, 
    location as office, 
    sf_office as office_location,
    utm_source, utm_medium, 
    CASE WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - National - Adv All Areas 0000 - Lead - CBO (Lifetime)' THEN 'Soc - Meta - Roofing - Prospecting - National - Adv All Areas 0000 - Lead - CBO (Lifetime) Campaign' 
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - Local - Charlotte 0049- Lead CBO (Lifetime)' THEN 'Soc - Meta - Roofing - Prospecting - Local - Charlotte 0049- Lead CBO (Lifetime) Cost Cap'
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - Local - Nashville 0004 - Lead CBO (Lifetime)' THEN 'Soc - Meta - Roofing - Prospecting - Local - Nashville 0025 - Lead CBO (Lifetime)'
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - Local - Orlando R086- Lead CBO (Lifetime)' THEN 'Soc - Meta - Roofing - Prospecting - Local - Orlando R086- Lead CBO (Lifetime) Cost Cap'
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - Local - Rochester 0010 - Lead CBO (Lifetime) - Copy' OR utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - Local - Rochester, NY 0010 - Lead CBO (Lifetime)' THEN 'Soc - Meta - Roofing - Prospecting - Local - Rochester NY 0010 - Lead CBO (Lifetime)'
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - National - VBLAL Test All Areas 0000 - Lead - Copy' THEN 'Soc - Meta - Roofing - Prospecting - National - VBLAL Test All Areas 0000 - Lead - 1DC'
        WHEN utm_campaign_adj ~* 'Meta' AND utm_campaign_adj ~* 'Roofing' AND utm_campaign_adj ~* 'Retargeting' AND utm_campaign_adj ~* 'National' AND utm_campaign_adj ~* 'All Areas Warm Test' AND utm_campaign_adj !~* 'Old Structure' AND utm_campaign_adj ~* 'CBO' THEN 'Soc - Meta - Roofing - Retargeting - National - All Areas Warm Test - Lead - CBO (Lifetime) Cost Cap' 
        WHEN utm_campaign_adj ~* 'Meta' AND utm_campaign_adj ~* 'Roofing' AND utm_campaign_adj ~* 'Retargeting' AND utm_campaign_adj ~* 'National' AND utm_campaign_adj ~* 'All Areas Warm' AND utm_campaign_adj !~* 'Old Structure' AND utm_campaign_adj ~* 'CBO' THEN 'Soc - Meta - Roofing - Retargeting - National - All Areas Warm - Lead - CBO (Lifetime) Cost Cap' 
        WHEN utm_campaign_adj ~* 'Meta' AND utm_campaign_adj ~* 'Roofing' AND utm_campaign_adj ~* 'Retargeting' AND utm_campaign_adj ~* 'National' AND utm_campaign_adj ~* 'All Areas Warm Test' AND utm_campaign_adj ~* 'Old Structure' AND utm_campaign_adj ~* 'CBO' THEN 'Soc - Meta - Roofing - Retargeting - National - Old Structure All Areas Warm Test - Lead - CBO (Lifetime) Cost Cap'
        WHEN utm_campaign_adj ~* 'Meta' AND utm_campaign_adj ~* 'Roofing' AND utm_campaign_adj ~* 'Retargeting' AND utm_campaign_adj ~* 'National' AND utm_campaign_adj ~* 'All Areas Warm' AND utm_campaign_adj ~* 'Old Structure' AND utm_campaign_adj ~* 'CBO' THEN 'Soc - Meta - Roofing - Retargeting - National - Old Structure All Areas Warm - Lead - CBO (Lifetime) Cost Cap'
        WHEN utm_campaign_adj ~* 'Soc - Meta - Roofing - Prospecting - National - Region 1 - Instant Form' THEN 'Soc - Meta - Roofing - Prospecting - National - All Areas - Region 1 - Instant Form'     
        ELSE utm_campaign_adj 
    END as utm_campaign, 
    utm_term, utm_content_adj as utm_content, utm_keyword, utm_match_type, utm_placement, utm_discount, utm_lp_variant,utm_campaign_id,
    leads,
    calls,
    appointments,
    demos,
    hits,
    issues,
    down_payments,
    closed_deals,
    gross,
    net,
    workable_leads,
    ooa_leads,
    net_sale_count,
    set_value
FROM final_data
LEFT JOIN office_data ON final_data.market_adj = office_data.sf_office 
