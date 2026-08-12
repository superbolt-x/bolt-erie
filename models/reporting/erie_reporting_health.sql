{{ config (
    alias = target.database + '_reporting_health'
)}}

{#
    Erie reporting health — the standing "is it safe to publish a number?" check.

    Modelled on reporting.fabric_reporting_health. EMPTY MEANS EVERYTHING PASSED. Rows with
    severity = 'fail' mean a client-facing number is wrong right now; `detail` says what to
    do about it.

    The point of this model is that it regenerates from the warehouse, so it cannot go stale
    the way a document can. Anything checkable here should be deleted from the erie-client
    skill's static warnings rather than duplicated.

    Deliberately NOT checked (confirmed dead or owned elsewhere, 2026-08-11):
      - landing-page models and the lp_variant 50/50 split      -> team no longer uses LP data
      - nextdoor / tiktok / outbrain / spend_files staleness    -> channels retired
      - facebook age_gender + campaign_location staleness       -> no longer used
      - googleads_searchterm_performance staleness              -> superseded by *_ytd_insights
      - the *_for_blended legacy views                          -> unused by any live consumer
#}

WITH

{# ---------------------------------------------------------------- 1. freshness #}
freshness AS (
    SELECT 'blended'    AS source, MAX(date) AS max_date, 1 AS tolerance_days FROM {{ this.schema }}.{{ target.database }}_blended_performance      WHERE date_granularity = 'day'
    UNION ALL
    SELECT 'googleads', MAX(date), 1 FROM {{ this.schema }}.{{ target.database }}_googleads_campaign_performance WHERE date_granularity = 'day'
    UNION ALL
    SELECT 'bingads',   MAX(date), 1 FROM {{ this.schema }}.{{ target.database }}_bingads_campaign_performance   WHERE date_granularity = 'day'
    UNION ALL
    SELECT 'facebook',  MAX(date), 1 FROM {{ this.schema }}.{{ target.database }}_facebook_performance_by_campaign WHERE date_granularity = 'day'
    UNION ALL
    {# The CRM daily file lands a day behind by design, so it gets a wider tolerance. #}
    SELECT 'salesforce', MAX(date), 2 FROM {{ this.schema }}.{{ target.database }}_salesforce_performance WHERE date_granularity = 'day'
    UNION ALL
    SELECT 'googleads_searchterm_ytd', MAX(date), 2 FROM {{ this.schema }}.{{ target.database }}_googleads_searchterm_ytd_insights
),

freshness_checks AS (
    SELECT 'STALE_SOURCE'  AS check_name,
           CASE WHEN DATEDIFF(day, max_date, CURRENT_DATE) > tolerance_days + 1 THEN 'fail' ELSE 'warn' END AS severity,
           source          AS entity,
           'Last row is ' || max_date::VARCHAR || ' (' || DATEDIFF(day, max_date, CURRENT_DATE)::VARCHAR
             || ' days back, tolerance ' || tolerance_days::VARCHAR
             || '). Do not publish a number that implies it covers today. Check /home/ubuntu/logs/erie.' AS detail
    FROM freshness
    WHERE DATEDIFF(day, max_date, CURRENT_DATE) > tolerance_days
),

{# ---------------------------------------------------------------- 2. unmapped funnel #}
{#  R12: an ad account absent from the erie_type CASE used to yield NULL and vanish from
    BOTH funnels. It now yields 'Unmapped', which this check surfaces. #}
unmapped AS (
    SELECT 'UNMAPPED_FUNNEL' AS check_name,
           'fail'            AS severity,
           channel || ' / ' || COALESCE(utm_campaign, '(no campaign)') AS entity,
           'Spend of $' || ROUND(SUM(spend))::VARCHAR || ' since ' || MIN(date)::VARCHAR
             || ' has erie_type = Unmapped, so it is missing from BOTH funnels. Add the ad '
             || 'account to the erie_type CASE in the relevant bolt-erie model.' AS detail
    FROM {{ this.schema }}.{{ target.database }}_blended_performance
    WHERE date_granularity = 'day'
      AND date >= DATEADD(day, -90, CURRENT_DATE)
      AND COALESCE(erie_type, 'Unmapped') NOT IN ('Roofing', 'Basement')
    GROUP BY 1,2,3
    HAVING SUM(spend) > 0
),

{# ---------------------------------------------------------------- 3. Basement CRM feed #}
{#  INVERTED 2026-08-12. This check used to fire when Basement CRM data APPEARED, because the
    daily file was Roofing-only and anything showing up meant a misclassified source code.
    The Erie team has now added Basement to the file, so the failure mode flipped: the risk
    is the feed silently STOPPING, which would quietly return Basement to in-platform-only
    reporting without anyone noticing.

    Checks each Basement channel that has meaningful spend and asks whether CRM leads are
    still arriving. Meta is excluded: Basement Meta CRM volume is small and lumpy enough that
    a quiet fortnight is normal rather than a fault.  #}
basement_crm AS (
    SELECT 'BASEMENT_CRM_STOPPED' AS check_name,
           'fail'                 AS severity,
           channel                AS entity,
           'Basement/' || channel || ' has $' || ROUND(SUM(spend))::VARCHAR || ' of spend in the '
             || 'last 30 days but ' || SUM(sf_leads)::VARCHAR || ' CRM leads. The daily file has '
             || 'carried Basement since 2026-08-12, so this means the feed stopped, prod stopped '
             || 'being set, or the erie_type derivation regressed. Basement reporting silently '
             || 'falls back to in-platform-only until it is fixed.' AS detail
    FROM {{ this.schema }}.{{ target.database }}_blended_performance
    WHERE date_granularity = 'day'
      AND date >= DATEADD(day, -30, CURRENT_DATE)
      AND erie_type = 'Basement'
      AND channel IN ('Google', 'Bing')
    GROUP BY 1,2,3
    HAVING SUM(spend) > 10000 AND SUM(sf_leads) = 0
),

{# ---------------------------------------------------------------- 4. Meta funnel wiring #}
{#  R30 mapped Meta's custom pixels into blended. If Meta ever shows material spend with
    zero sets again, the pixel mapping has regressed (or the pixel stopped firing).  #}
meta_wiring AS (
    SELECT 'META_FUNNEL_UNWIRED' AS check_name,
           'fail'                AS severity,
           'Facebook / ' || erie_type AS entity,
           'Meta has $' || ROUND(SUM(spend))::VARCHAR || ' of spend in the last 30 days but '
             || ROUND(SUM(inplatform_appointments))::VARCHAR || ' sets. The fb_pixel_custom.set '
             || 'mapping in facebook_sub_sources.sql has regressed, or the pixel stopped firing.' AS detail
    FROM {{ this.schema }}.{{ target.database }}_blended_performance
    WHERE date_granularity = 'day'
      AND date >= DATEADD(day, -30, CURRENT_DATE)
      AND channel = 'Facebook'
    GROUP BY 1,2,3
    HAVING SUM(spend) > 10000 AND SUM(inplatform_appointments) = 0
),

{# ---------------------------------------------------------------- 5. granularity drift #}
{#  R11 fixed a fan-out that made conversions grow with bucket size. This re-checks the
    invariant directly: a month bucket must equal the days inside it.  #}
gran_drift AS (
    SELECT 'GRANULARITY_DRIFT' AS check_name,
           'fail'              AS severity,
           channel || ' / ' || metric AS entity,
           'Month-grain ' || metric || ' is ' || ROUND(month_val)::VARCHAR || ' but the days inside '
             || 'that month sum to ' || ROUND(day_val)::VARCHAR || ' (' || ROUND(pct_diff, 1)::VARCHAR
             || '% drift) for ' || month_start::VARCHAR
             || '. A join fan-out has returned; see googleads_sub_sources.sql url_split.' AS detail
    FROM (
        SELECT d.channel, d.month_start, 'inplatform_leads' AS metric,
               d.day_val, m.month_val,
               ABS(m.month_val - d.day_val) / NULLIF(d.day_val, 0) * 100 AS pct_diff
        FROM (SELECT channel, DATE_TRUNC('month', date) AS month_start, SUM(inplatform_leads) AS day_val
              FROM {{ this.schema }}.{{ target.database }}_blended_performance
              WHERE date_granularity = 'day'
                AND date >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE))
                AND date <  DATE_TRUNC('month', CURRENT_DATE)
              GROUP BY 1,2) d
        JOIN (SELECT channel, date AS month_start, SUM(inplatform_leads) AS month_val
              FROM {{ this.schema }}.{{ target.database }}_blended_performance
              WHERE date_granularity = 'month'
                AND date >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE))
                AND date <  DATE_TRUNC('month', CURRENT_DATE)
              GROUP BY 1,2) m
          ON d.channel = m.channel AND d.month_start = m.month_start
    )
    WHERE pct_diff > 1
),

{# ---------------------------------------------------------------- 6. dimension domains #}
{#  Cheap canaries: a new campaign_type or channel literal appearing means a downstream
    filter somewhere is now silently dropping rows.  #}
domain_drift AS (
    SELECT 'NEW_DIMENSION_VALUE' AS check_name,
           'warn'                AS severity,
           'campaign_type = ' || COALESCE(campaign_type, '(null)') AS entity,
           'Unrecognised campaign_type carrying $' || ROUND(SUM(spend))::VARCHAR
             || ' in the last 30 days. Any question filtering campaign_type is now dropping it.' AS detail
    FROM {{ this.schema }}.{{ target.database }}_blended_performance
    WHERE date_granularity = 'day'
      AND date >= DATEADD(day, -30, CURRENT_DATE)
      AND COALESCE(campaign_type, '(null)') NOT IN (
            'Search', 'Performance Max', 'Demand Gen', 'Prospecting', 'Retargeting',
            'Audience', 'Campaign Type: Prospecting', '(null)')
    GROUP BY 1,2,3
    HAVING SUM(spend) > 0
),

{# ---------------------------------------------------------------- 7. zero-spend channels #}
{#  Channels carrying CRM leads at $0 spend make any all-channel CPL infinite. Informational,
    but it is the most common cause of a nonsensical blended efficiency number.  #}
zero_spend AS (
    SELECT 'CRM_LEADS_NO_SPEND' AS check_name,
           'warn'               AS severity,
           channel              AS entity,
           channel || ' carries ' || SUM(sf_leads)::VARCHAR || ' CRM leads at $0 spend in the '
             || 'last 30 days. Exclude it from all-channel CPL/CAC or the rate goes infinite.' AS detail
    FROM {{ this.schema }}.{{ target.database }}_blended_performance
    WHERE date_granularity = 'day'
      AND date >= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY 1,2,3
    HAVING SUM(sf_leads) > 0 AND SUM(spend) = 0
)

SELECT check_name, severity, entity, detail FROM freshness_checks
UNION ALL SELECT check_name, severity, entity, detail FROM unmapped
UNION ALL SELECT check_name, severity, entity, detail FROM basement_crm
UNION ALL SELECT check_name, severity, entity, detail FROM meta_wiring
UNION ALL SELECT check_name, severity, entity, detail FROM gran_drift
UNION ALL SELECT check_name, severity, entity, detail FROM domain_drift
UNION ALL SELECT check_name, severity, entity, detail FROM zero_spend
ORDER BY CASE severity WHEN 'fail' THEN 0 ELSE 1 END, check_name, entity
