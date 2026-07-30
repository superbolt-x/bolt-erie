{{ config (
    materialized = 'ephemeral'
)}}

{#
    ad_final_urls -> sub_source_id.

    The sub source code is the tail of the landing page URL, but the tail
    format varies, so it gets peeled off in three passes. Logic is carried over
    unchanged from the models this replaces; only the shape changed (nested
    subqueries instead of lateral alias references inside a GROUP BY) so the
    three passes are readable.

    One row per ad_final_urls, so joining it can never fan out.
#}

WITH url_tail as (
    SELECT DISTINCT
        ad_final_urls,
        CASE
            WHEN RIGHT(ad_final_urls, 5) = 'tep/]' THEN LEFT(RIGHT(ad_final_urls, 16),3)
            WHEN (ad_final_urls = '[http://go.eriemetalroofs.com/erie-youtube-metal-roofing-f/]'
                or ad_final_urls = '[https://go.eriemetalroofs.com/erie-nn/]'
                or ad_final_urls = '[https://go.eriemetalroofs.com/erie-youtube-metal-roofing-f/]'
                or ad_final_urls ~* 'https://www.eriehome.com/lp/metal-roofing/?utm_source=r-dgd')
                THEN 'other'
            ELSE RIGHT(ad_final_urls, 5)
        END as test
    FROM {{ source('googleads_raw','ad_performance_report') }}
),

trimmed_tail as (
    SELECT
        ad_final_urls,
        CASE
            WHEN test ~* '/]' THEN left(test,3)
            WHEN test ~* ']' THEN left(test,4)
            ELSE test
        END as test2
    FROM url_tail
)

SELECT
    ad_final_urls,
    CASE
        WHEN test2 ~* '=' and test2 !~* '=48' THEN right(test2,3)
        WHEN test2 ~* '=' and test2 ~* '=48' THEN right(test2,2)
        WHEN test2 ~* '-' THEN right(test2,2)
        ELSE test2
    END as sub_source_id
FROM trimmed_tail
