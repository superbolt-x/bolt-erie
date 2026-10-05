{#  Meta campaign_type from the campaign name. NULL when the name has no signal,
    so each model keeps its own fallback. #}

{% macro erie_meta_campaign_type(name_col) %}
    CASE WHEN {{ name_col }} ~* 'warm|retargeting' THEN 'Retargeting'
         WHEN {{ name_col }} ~* 'prospecting' THEN 'Prospecting'
    END
{% endmacro %}
