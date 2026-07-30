{#
    Canonical metric contract for the blended_performance union.

    blended_performance UNIONs its channel models by position, so every model
    has to emit the same metric columns in the same order. This macro is the
    single place that order is defined: pass the metrics the channel actually
    has as complete SQL expressions, and everything else is emitted as 0.

    Usage:
        {{ blended_metrics({
            'spend': 'COALESCE(SUM(spend),0)',
            'clicks': 'COALESCE(SUM(link_clicks),0)'
        }) }}
#}

{% macro blended_metrics(overrides={}) %}

    {%- set metrics = [
        'spend',
        'clicks',
        'impressions',
        'inplatform_leads',
        'video_views',
        'sf_leads',
        'calls',
        'appointments',
        'demos',
        'down_payments',
        'closed_deals',
        'gross',
        'net',
        'workable_leads',
        'hits',
        'issues',
        'ooa_leads',
        'net_sale_count',
        'inplatform_workable_leads',
        'inplatform_appointments',
        'set_value',
        'inplatform_issues',
        'inplatform_net',
        'inplatform_net_sale_count',
        'inplatform_set_value',
        'inplatform_kashurba_leads',
        'gross_sale_count',
        'inplatform_conversions'
    ] -%}

    {#- A typo in an override key would otherwise be silently dropped, which is
        exactly the failure this macro exists to prevent. -#}
    {%- for column in overrides -%}
        {%- if column not in metrics -%}
            {{ exceptions.raise_compiler_error(
                "blended_metrics: '" ~ column ~ "' is not part of the blended metric contract"
            ) }}
        {%- endif -%}
    {%- endfor -%}

    {%- for metric in metrics %}
        {{ overrides.get(metric, '0') }} AS {{ metric }}{{ "," if not loop.last }}
    {%- endfor %}

{% endmacro %}
