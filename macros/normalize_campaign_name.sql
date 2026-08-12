{#
    Campaign-name join key.

    Roofing leads carry a numeric utm_campaign_id, so salesforce_sub_sources joins them to
    the platform campaign tables on the id. BASEMENT LEADS DO NOT — measured 2026-08-12 on
    s3_raw.superbolt_daily_file where prod = 'Basement Waterproofing': utm_campaign_id is
    populated on 0 of 14,230 facebook rows, 5 of 11,710 meta rows, 591 of 8,777 google rows
    and 77 of 888 bing rows. What Basement carries instead is the campaign NAME, so it has
    to be joined by name.

    A raw name join is not enough, because the same campaign is spelled several ways:

      platform  'B002 - Cincinnati - General Search  (Incline)'   <- two spaces
      utm       'B002 - Cincinnati - General Search (Incline)'    <- one
      utm       '0000_national_search_foundation_repair'          <- underscored, lowercased
      utm       '0000%20-%20National%20-%20Brand%20%28Incline%29' <- URL-encoded

    Stripping every non-alphanumeric after decoding the handful of escapes Salesforce emits
    collapses all four to the same key. Measured effect on May-Aug 2026 Basement leads:
    Google 75.0% -> 94.9% matched, Bing 96.5% -> 97.1%, Meta 100% -> 100%.

    The ~5% that still miss are not campaigns at all — 'Google Ads Extension',
    'Microsoft Ads Extension', 'Google Local Services Ads Basement', a Google Business
    Profile listing. Those have no spend row to join to and are correctly left unmatched.

    Usage:  {{ normalize_campaign_name('utm_campaign') }}
#}

{% macro normalize_campaign_name(column) %}
    REGEXP_REPLACE(
        LOWER(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                {{ column }},
                '%20', ' '), '%28', '('), '%29', ')'), '%26', '&'), '%2B', '+'), '%3A', ':'), '%2F', '/')
        ),
        '[^a-z0-9]', ''
    )
{% endmacro %}
