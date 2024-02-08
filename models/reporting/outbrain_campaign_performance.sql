{{ config (
    alias = target.database + '_outbrain_campaign_performance_test'
)}}
    
{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
{% set date_parts = get_date_parts('date') %}
    
WITH campaign_data as 
    (SELECT id, name as campaign_name, last_modified, max(last_modified) over (partition by id) as last_updated_date FROM {{ source('outbrain_raw','campaign_history') }} )
    , campaign_names as (SELECT id as campaign_id, campaign_name FROM campaign_data WHERE last_modified = last_updated_date)
    ,  campaign_insights_data as 
    (SELECT *, max(api_sync_timestamp) over (partition by campaign_id,date) as last_updated_date FROM {{ source('gsheet_raw','outbrain_campaign_insights') }} )
    ,  campaign_insights as 
    (SELECT * FROM campaign_insights_data where api_sync_timestamp=last_updated_date )

{%- for date_granularity in date_granularity_list %}    
    SELECT 
    {% if date_granularity == 'day' %}
            {{ date_parts.day }} AS date
        {% elif date_granularity == 'week' %}
            {{ date_parts.week }} AS date
        {% elif date_granularity == 'month' %}
            {{ date_parts.month }} AS date
        {% elif date_granularity == 'quarter' %}
            {{ date_parts.quarter }} AS date
        {% elif date_granularity == 'year' %}
            {{ date_parts.year }} AS date
        {% endif %}, '{{date_granularity}}' as date_granularity,
      campaign_id,
      campaign_name,
      --platform,
      --publisher,
      --audience,
      --location,
      COALESCE(SUM(spend),0) as spend,
      COALESCE(SUM(clicks),0) as clicks,
      COALESCE(SUM(impressions),0) as impressions,
      COALESCE(SUM(total_conversions),0) as conversions,
      COALESCE(SUM(total_conversion_value),0) as conversions_value,
      COALESCE(SUM(total_erie_lead),0) as leads
    FROM campaign_insights
    LEFT JOIN campaign_names USING(campaign_id)
    GROUP BY 1,2,3,4
    {% if not loop.last %}UNION ALL
            {% endif %}
{% endfor %}
