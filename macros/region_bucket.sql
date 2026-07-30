{#
    Region buckets, matched against one or more columns.

    Same single-column-or-list convention as service_type_bucket.

    Usage:
        {{ region_bucket('campaign_name') }}
        {{ region_bucket(['bg_campaign_name::VARCHAR', 'utm_campaign']) }}
#}

{% macro region_bucket(columns) %}

    {%- set column_list = [columns] if columns is string else columns -%}
    {%- set regions = ['All areas', 'Group', 'National'] -%}

    CASE
        {%- for region in regions %}
        WHEN {% for column in column_list %}{{ column }} ~* '{{ region }}'{{ " OR " if not loop.last }}{% endfor %} THEN '{{ region }}'
        {%- endfor %}
        ELSE 'Other'
    END

{% endmacro %}
