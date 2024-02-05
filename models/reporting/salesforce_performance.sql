{{ config (
    alias = target.database + '_salesforce_performance'
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
    ORDER BY code ASC),
    
    filetered_data as
    (SELECT *
    FROM {{ source('snowflake_superbolt','superbolt_daily_file') }}
    WHERE _fivetran_deleted IS false),

    final_data as 
    (SELECT DATE_TRUNC('day',lead_entry_date::date) as date, 'day' as date_granularity,
        market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, utm_campaign, utm_term, 
        CASE WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN REPLACE(REPLACE(utm_content,'_',' '),'-','')::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount,
        COUNT(DISTINCT lead_id) as leads,
        COALESCE(SUM(number_of_calls),0) as calls,
        COALESCE(SUM("set"),0) as appointments,
        COALESCE(SUM(demo),0) as demos,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issued),0) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        COALESCE(SUM(sold),0) as closed_deals,
        COALESCE(SUM("gross__"),0) as gross,
        COALESCE(SUM("net__"),0) as net,
        COUNT(DISTINCT lead_id)-(COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Wrong Number' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Duplicate Record' THEN lead_id END)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads
    FROM filetered_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',lead_entry_date::date+1)-1 as date, 'week' as date_granularity,
        market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, utm_campaign, utm_term, 
        CASE WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN REPLACE(REPLACE(utm_content,'_',' '),'-','')::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount,
        COUNT(DISTINCT lead_id) as leads,
        COALESCE(SUM(number_of_calls),0) as calls,
        COALESCE(SUM("set"),0) as appointments,
        COALESCE(SUM(demo),0) as demos,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issued),0) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        COALESCE(SUM(sold),0) as closed_deals,
        COALESCE(SUM("gross__"),0) as gross,
        COALESCE(SUM("net__"),0) as net,
        COUNT(DISTINCT lead_id)-(COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Wrong Number' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Duplicate Record' THEN lead_id END)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads
    FROM filetered_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',lead_entry_date::date) as date, 'month' as date_granularity,
        market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, utm_campaign, utm_term, 
        CASE WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN REPLACE(REPLACE(utm_content,'_',' '),'-','')::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount,
        COUNT(DISTINCT lead_id) as leads,
        COALESCE(SUM(number_of_calls),0) as calls,
        COALESCE(SUM("set"),0) as appointments,
        COALESCE(SUM(demo),0) as demos,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issued),0) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        COALESCE(SUM(sold),0) as closed_deals,
        COALESCE(SUM("gross__"),0) as gross,
        COALESCE(SUM("net__"),0) as net,
        COUNT(DISTINCT lead_id)-(COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Wrong Number' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Duplicate Record' THEN lead_id END)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads
    FROM filetered_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',lead_entry_date::date) as date, 'quarter' as date_granularity,
        market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, utm_campaign, utm_term, 
        CASE WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN REPLACE(REPLACE(utm_content,'_',' '),'-','')::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount,
        COUNT(DISTINCT lead_id) as leads,
        COALESCE(SUM(number_of_calls),0) as calls,
        COALESCE(SUM("set"),0) as appointments,
        COALESCE(SUM(demo),0) as demos,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issued),0) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        COALESCE(SUM(sold),0) as closed_deals,
        COALESCE(SUM("gross__"),0) as gross,
        COALESCE(SUM("net__"),0) as net,
        COUNT(DISTINCT lead_id)-(COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Wrong Number' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Duplicate Record' THEN lead_id END)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads
    FROM filetered_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',lead_entry_date::date) as date, 'year' as date_granularity,
        market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail, 
        utm_source, utm_medium, utm_campaign, utm_term, 
        CASE WHEN source IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') OR utm_source = 'youtube' THEN REPLACE(REPLACE(utm_content,'_',' '),'-','')::VARCHAR ELSE utm_content END as utm_content_adj,
        utm_keyword, utm_match_type, utm_placement, utm_discount,
        COUNT(DISTINCT lead_id) as leads,
        COALESCE(SUM(number_of_calls),0) as calls,
        COALESCE(SUM("set"),0) as appointments,
        COALESCE(SUM(demo),0) as demos,
        COALESCE(SUM(hits),0) as hits,
        COALESCE(SUM(issued),0) as issues,
        COALESCE(SUM(CASE WHEN paid_out_date IS NOT NULL THEN 1 ELSE 0 END),0) as down_payments,
        COALESCE(SUM(sold),0) as closed_deals,
        COALESCE(SUM("gross__"),0) as gross,
        COALESCE(SUM("net__"),0) as net,
        COUNT(DISTINCT lead_id)-(COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Wrong Number' THEN lead_id END)+COUNT(DISTINCT CASE WHEN status_detail ~* 'Duplicate Record' THEN lead_id END)) as workable_leads,
        COUNT(DISTINCT CASE WHEN market = '999 - Invalid' THEN lead_id END) as ooa_leads
    FROM filetered_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)

SELECT 
    date,
    date_granularity,
    market, state, source,zip, sub_source_id, sub_source, dispo, call_disposition, status_detail, 
    location as office, 
    sf_office as office_location,
    utm_source, utm_medium, utm_campaign, utm_term, utm_content_adj as utm_content, utm_keyword, utm_match_type, utm_placement, utm_discount,
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
    ooa_leads
FROM final_data
LEFT JOIN office_data ON final_data.market = office_data.sf_office 
