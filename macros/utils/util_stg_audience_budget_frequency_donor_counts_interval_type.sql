-- fmt: off
{% macro util_stg_audience_budget_frequency_donor_counts_interval_type (
    frequency,
    interval,
    budget_table = "stg_audience_budget_by_day"
)%}

    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'week','month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}

     {% set recur_onetime = "recur" if frequency == "recurring" else "onetime" %}

    {{
    config(
        materialized="table",
        partition_by={
                "field": "date_day",
                "data_type": "date",
                "granularity": "day",
            },
        cluster_by='donor_audience'
    )
}}

    with
        sums as (
            select

    {% if interval == 'day' %}
                date(date_day) as date_day,
                'daily' as interval_type,
    {% elif interval == 'month' %}
                last_day(date_day, month) as date_day,
                'monthly' as interval_type,
    {% elif interval == 'year' %}
                last_day(date_day, year) as date_day,
                'yearly' as interval_type,
    {% endif %}
                donor_audience,
                platform as channel,
                sum(total_revenue_budget_by_day) as {{recur_onetime}}_donor_count_budget,
                sum(loyalty_new_donor_targets_by_day) as {{recur_onetime}}_new_donor_count_budget
            from {{ ref(budget_table) }}
            where
    {% if frequency == 'onetime' %}
                lower(donor_audience) != 'recurring'
                and lower(donor_audience) != 'monthly'
    {% elif frequency == 'recurring' %}
                lower(donor_audience) = 'recurring'
                or lower(donor_audience) = 'monthly'
    {% endif %}
            group by 1, 2, 3, 4
        )

    select
        date_day,
        interval_type,
        donor_audience,
        channel,
        {{recur_onetime}}_donor_count_budget,
        {{recur_onetime}}_new_donor_count_budget,
        sum({{recur_onetime}}_donor_count_budget) over (
            partition by
                donor_audience,
                channel,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }}
            order by date_day
        ) as {{recur_onetime}}_donor_count_budget_cumulative,
        sum({{recur_onetime}}_new_donor_count_budget) over (
            partition by
                donor_audience,
                channel,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }}
            order by date_day
        ) as {{recur_onetime}}_new_donor_count_cumulative
    from sums

{% endmacro %}
