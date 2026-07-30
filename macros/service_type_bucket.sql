{#
    Roofing service buckets, matched against one or more columns.

    Accepts a single column name or a list of them; a list matches if any
    column matches, which is what the Salesforce model needs (it checks both
    the joined ad group name and the utm_term).

    Usage:
        {{ service_type_bucket('ad_group_name') }}
        {{ service_type_bucket(['gb_ad_group_name::VARCHAR', 'utm_term']) }}
#}

{% macro service_type_bucket(columns) %}

    {%- set column_list = [columns] if columns is string else columns -%}
    {%- set services = [
        'Roof Replacement',
        'General Roofing',
        'Residential Roofing',
        'Metal Roofing',
        'Steel Roofing',
        'Fiberglass Roofing',
        'Spanish Tiles'
    ] -%}

    CASE
        {%- for service in services %}
        WHEN {% for column in column_list %}{{ column }} ~* '{{ service }}'{{ " OR " if not loop.last }}{% endfor %} THEN '{{ service }}'
        {%- endfor %}
        ELSE 'Other'
    END

{% endmacro %}
