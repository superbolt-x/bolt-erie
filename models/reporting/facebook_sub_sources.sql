{{ config (
    materialized = 'ephemeral'
)}}

SELECT
        'Facebook' AS channel,
        date, 
        date_granularity, 
        office, 
        office_location, 
        NULL as sf_locations, 
        NULL as source,
        NULL as sub_source_id, 
        NULL as sub_source,
        NULL as zip, 
        CASE WHEN (account_id = '813620678687014' OR account_id = '306770030564777') THEN 'Roofing' 
             WHEN account_id = '1349056908916556' THEN 'Basement'
             ELSE 'Unmapped'
        END as erie_type,
        CASE WHEN campaign_name = 'Soc - Meta - Roofing - Prospecting - National - Paused Local Campaigns - Lead - Instant Form'
                or (campaign_name ~* 'All Area' and (account_id = '813620678687014' OR account_id = '306770030564777') ) 
                or ((campaign_name ~* 'sandbox' or campaign_name ~* 'All Area') and account_id = '1349056908916556') 
				or (campaign_name ~* 'national') 
             THEN 'National'
             WHEN ((campaign_name !~* 'All Area' and (account_id = '813620678687014' OR account_id = '306770030564777') ) 
                or ((campaign_name !~* 'sandbox' or campaign_name !~* 'All Area') and account_id = '1349056908916556')) AND campaign_name ~* 'Local' 
             THEN 'Local'
        END as market,
        CASE WHEN campaign_name !~* 'warm' THEN 'Prospecting' 
            WHEN campaign_name ~* 'warm' THEN 'Retargeting' 
            WHEN campaign_name ~* 'LP Clicks Traffic' THEN 'Traffic' 
            WHEN campaign_name ~* 'LP Views Leads' THEN 'View Content' 
        END as campaign_type,
        {{ region_bucket('campaign_name') }} as region_bucket,
        {{ service_type_bucket('adset_name') }} as service_type,
        NULL as dispo,
        NULL as call_disposition,
        NULL as status_detail,
        NULL as utm_medium,
        CASE
          WHEN campaign_id = 6930690664241 THEN 'Soc - Meta - Roofing - Prospecting - National - Florida Regional - Lead - CBO (Lifetime) Campaign'
	  	  WHEN campaign_id = 6931185343441 THEN 'Soc - Meta - Roofing - Prospecting - National - Great Lakes and East Great Lakes Regional - Instant Form - Lifetime'
	  	  WHEN campaign_id = 6930683609841 THEN 'Soc - Meta - Roofing - Prospecting - National - Northeast Regional - Lead - CBO (Lifetime) Campaign'
        ELSE campaign_name::VARCHAR END as utm_campaign,
        adset_name::VARCHAR as utm_term,
        ad_name::VARCHAR as utm_content,
        NULL as utm_keyword,
        NULL as utm_match_type,
        NULL as utm_placement,
        NULL as utm_discount,
        NULL as utm_lp_variant,
		NULL as utm_msclk_id,
        {#
            Meta's down-funnel comes from custom PIXELS, not from conversion actions
            (Erie never set those up on Meta the way they did on Google and Bing). The
            pixels are exposed by facebook_ad_performance and mapped here so Meta stops
            reading as a literal 0 in blended: before this, July 2026 showed $1.31M of
            Meta spend against 0 sets / 0 issues / 0 net sales, which reads as a failing
            channel rather than an unwired one.

            inplatform_leads uses website_leads (the pixel-only count), NOT `leads`
            (= website_leads + onfacebook_leads), because the latter double-counts a
            single Instant Form submission. Verified against the Basement weekly report:
            website_leads gives 368 for July, matching the published CPL of $130;
            `leads` gives 731.

            Every Meta pixel is a conversion COUNT — there are no value-bearing pixels.
            So inplatform_net (a revenue VALUE on Google/Bing, from [roofing]netsale_value)
            and inplatform_set_value stay 0 here rather than being fed a count, which would
            silently make Meta Net COM and Set ROAS wrong. Meta also has no workable-lead
            pixel, so inplatform_workable_leads stays 0 — that remains a Tableau pull.

            The `.sale` pixel is deliberately NOT mapped: it is ambiguous between gross and
            net sale, and `.net sale` already covers the net count.
        #}
        {{ blended_metrics({
            'spend': 'COALESCE(SUM(spend),0)',
            'clicks': 'COALESCE(SUM(link_clicks),0)',
            'impressions': 'COALESCE(SUM(impressions),0)',
            'inplatform_leads': 'COALESCE(SUM(website_leads),0)',
            'inplatform_appointments': 'COALESCE(SUM(appointment_set),0)',
            'inplatform_issues': 'COALESCE(SUM(pixel_issues),0)',
            'inplatform_net_sale_count': 'COALESCE(SUM(pixel_net_sales),0)',
            'inplatform_kashurba_leads': 'COALESCE(SUM(pixel_kashurba_leads),0)'
        }) }}
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE date >= '2022-12-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
