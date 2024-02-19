-- fmt: off
{% macro util_stg_audience_budget_frequency_donor_counts_interval_type (
    frequency,
    interval,
    budget_table = "stg_audience_budget_by_day"
)%}

    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}

     {% set recur_onetime = "recur" if frequency == "recurring" else "onetime" %}

    with
        base as (
            select

    {% if interval == 'day' %}
                date_day,
                'daily' as interval_type,
    {% elif interval == 'month' %}
                date(extract (year from date_day), extract(month from date_day), 1) as date_day,
                'monthly' as interval_type,
    {% elif interval == 'year' %}
                date(extract (year from date_day), 1, 1) as date_day,
                'yearly' as interval_type,
    {% endif %}
                donor_audience,
                platform as join_source,
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
        ),

distinct_audiences as (
    select distinct donor_audience from base
),

distinct_channels as (
    select distinct join_source from base
),

distinct_days as (
    select distinct date_day from base
),

cross_join as (
    select 
        date_day,
        donor_audience,
        channel 
    from distinct_days 
    cross join distinct_channels
    cross join distinct_audiences
),


true_cumulative as (
    select 
        cross_join.date_day,
        base.fiscal_year,
        base.interval_type,
        cross_join.donor_audience,
        cross_join.channel,
        sum({{recur_onetime}}_donor_count_budget) over (
            partition by
                donor_audience,
                join_source,
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
                join_source,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }}
            order by date_day
        ) as {{recur_onetime}}_new_donor_count_budget_cumulative
    from cross_join 
    full outer join base using (date_day, donor_audience, channel)
)


 select
        true_cumulative.date_day,
        true_cumulative.interval_type,
        true_cumulative.fiscal_year,
        true_cumulative.donor_audience,
        true_cumulative.join_source,
        base.{{recur_onetime}}_donor_count_budget,
        base.{{recur_onetime}}_new_donor_count_budget,
        true_cumulative.{{recur_onetime}}_donor_count_budget_cumulative,
        true_cumulative.{{recur_onetime}}_new_donor_count_budget_cumulative  
    from true_cumulative 
    full outer join base using (date_day, donor_audience, channel)

{% endmacro %}
