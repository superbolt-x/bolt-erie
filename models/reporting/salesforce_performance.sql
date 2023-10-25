{{ config (
    alias = target.database + '_salesforce_performance'
)}}

WITH final_data as
    (SELECT *
    FROM snowflake_superbolt.superbolt_daily_file
    WHERE (source = 'BPMX' OR _fivetran_deleted IS false))

SELECT DATE_TRUNC('day',lead_entry_date::date) as date, 'day' as date_granularity,
    market, state, source, zip,sub_source_id, sub_source, dispo, call_disposition, status_detail,
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
FROM final_data
where source = 'BPMX'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL

SELECT DATE_TRUNC('week',lead_entry_date::date) as date, 'week' as date_granularity,
    market, state, source,zip,sub_source_id, sub_source, dispo, call_disposition, status_detail,
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
FROM final_data
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL

SELECT DATE_TRUNC('month',lead_entry_date::date) as date, 'month' as date_granularity,
    market, state, source,zip,sub_source_id, sub_source, dispo, call_disposition, status_detail,
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
FROM final_data
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL

SELECT DATE_TRUNC('quarter',lead_entry_date::date) as date, 'quarter' as date_granularity,
    market, state, source,zip,sub_source_id, sub_source, dispo, call_disposition, status_detail,
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
FROM final_data
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL

SELECT DATE_TRUNC('year',lead_entry_date::date) as date, 'year' as date_granularity,
    market, state, source,zip, sub_source_id, sub_source, dispo, call_disposition, status_detail,
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
FROM final_data
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
