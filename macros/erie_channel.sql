{#
    Channel resolution for Salesforce lead rows.

    Two signals disagree occasionally and neither is a clean winner:

      `source`      the Salesforce source code (SM, BIL2, ...). 100% populated for the whole
                    history, but it is a hand-maintained code whose meaning drifts — BSM2 is
                    genuinely YouTube before 2025 (numeric YouTube campaign ids) and Google
                    after, with no code change.
      `utm_source`  what the click actually carried. 0% populated before 2023-12, 34.3% in
                    Dec 2023, then 92.9% in Jan 2024 and 94-99% ever since. More current, but
                    it often carries a vendor or affiliate name ('Incline Marketing',
                    'taboola', 'Modernize', 'Yelp') rather than an ad platform.

    Measured on Roof + Basement leads from 2024-01-01 (~500k):

      - Adopting utm_source wholesale would push 1,006 leads to 'Other' that `source`
        classifies correctly, because their utm_source is an affiliate name.
      - Both signals name a KNOWN platform and disagree on only ~241 leads. BSM2 is 97 of
        those — 40% of every such conflict in the file.

    So: `source` stays primary, and utm_source only breaks the tie when BOTH name a known
    platform, and only from the date utm_source became trustworthy. That reclassifies ~241
    leads, resolves BSM2 without a hand-maintained cut-over, and keeps the 1,006.

    UTM_RELIABLE_FROM is 2024-01-01: the first month above 90% (92.9%), after 0.4% in Nov
    2023 and 34.3% in Dec.
#}

{% macro erie_channel_from_source(source_col) %}
    CASE WHEN {{ source_col }} IN ('SM','SMR','SMO','SM1','SM13','BSM','BSMR','BSM1') THEN 'Facebook'
         WHEN {{ source_col }} IN ('SM2','SM4','RYT','BRYT','BSM2','BSM4') THEN 'YouTube'
         WHEN {{ source_col }} IN ('PMX','BPMX','IL2','SMD','BIL2','BSMD','IL5','BIL5') THEN 'Google'
         WHEN {{ source_col }} IN ('SM6','BSM6') THEN 'TikTok'
         WHEN {{ source_col }} IN ('IL3','BIL3','BNA','PMX2') THEN 'Bing'
         WHEN {{ source_col }} IN ('SM5','BSM5') THEN 'Nextdoor'
         WHEN {{ source_col }} IN ('SM3','BSM3') THEN 'Outbrain'
         WHEN {{ source_col }} IN ('YLP','BYLP') THEN 'Yelp'
    END
{% endmacro %}

{% macro erie_channel_from_utm(utm_source_col) %}
    {#  Matched case-insensitively: Basement writes 'facebook' (14,230), 'Facebook' (5,772),
        'meta' (11,710), 'fb' (301), 'ig' (56) and 'Meta' (10) for the same channel. #}
    CASE WHEN {{ utm_source_col }} ~* '(facebook|^meta$|^fb|^ig$|instagram)' THEN 'Facebook'
         WHEN {{ utm_source_col }} ~* 'youtube' THEN 'YouTube'
         WHEN {{ utm_source_col }} ~* 'google' THEN 'Google'
         WHEN {{ utm_source_col }} ~* 'tiktok' THEN 'TikTok'
         WHEN {{ utm_source_col }} ~* 'bing' THEN 'Bing'
         WHEN {{ utm_source_col }} ~* 'nextdoor' THEN 'Nextdoor'
         WHEN {{ utm_source_col }} ~* 'outbrain' THEN 'Outbrain'
         WHEN {{ utm_source_col }} ~* 'yelp' THEN 'Yelp'
    END
{% endmacro %}

{% macro erie_channel(source_col='source', utm_source_col='utm_source', date_col='date') %}
    {%- set from_source = erie_channel_from_source(source_col) -%}
    {%- set from_utm = erie_channel_from_utm(utm_source_col) -%}
    CASE
        {#  Tie-break: both signals name a known platform, and utm_source is trustworthy. #}
        WHEN {{ date_col }} >= '2024-01-01'
             AND ({{ from_utm }}) IS NOT NULL
             AND ({{ from_source }}) IS NOT NULL
            THEN ({{ from_utm }})
        WHEN ({{ from_source }}) IS NOT NULL THEN ({{ from_source }})
        WHEN ({{ from_utm }}) IS NOT NULL THEN ({{ from_utm }})
        ELSE 'Other'
    END
{% endmacro %}
