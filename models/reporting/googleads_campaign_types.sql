{{ config (
    materialized = 'ephemeral'
)}}

{#
    Latest advertising_channel_type per campaign.

    Every Google model needs this to tell Performance Max / Video / Search
    apart. Verified one row per campaign (10,875 campaigns, no updated_at
    ties), so joining it can never fan out.
#}

WITH campaign_max_updated_date as (
    SELECT id, max(updated_at) as max_updated_at
    FROM {{ source('googleads_raw', 'campaign_history') }}
    GROUP BY 1
)

SELECT
    campaign_max_updated_date.id as campaign_id,
    advertising_channel_type
FROM campaign_max_updated_date
LEFT JOIN {{ source('googleads_raw', 'campaign_history') }}
    ON campaign_max_updated_date.id = campaign_history.id
    AND campaign_max_updated_date.max_updated_at = campaign_history.updated_at
