{% macro util_mart_audience_retention_activation(recur_status, first_gift_table='stg_stitch_sfmc_audience_transaction_first_gift', transactions_table='stg_stitch_sfmc_audience_transaction_with_first_gift_cohort') %}


 {% if recur_status not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'recur_status' argument to util_mart_audience_retention_activation must be 'recurring' or 'onetime', got " ~ recur_status) }}
    {% endif %}

{% set recur_suffix = '_recur' if recur_status == 'recurring' else '' %}
{% set boolean_status = True if recur_status == 'recurring' else False %}
{% set ret_or_act = 'Ret' if recur_status == 'recurring' else 'Act' %}
{% set retention_or_activation = 'retention' if recur_status == 'recurring' else 'activation' %}
{% set donors_in_cohort = 'stg_stitch_sfmc_audience_transaction_first_gift_recur_rollup' if recur_status == 'recurring' else 'stg_stitch_sfmc_audience_transaction_first_gift_1x_rollup' %}


with first_gift_rollup as (
select
join_month_year_str,
first_gift_join_source,
join_gift_size_string{{recur_suffix}},
first_gift_donor_audience,
CAST(NULL as string) as {{retention_or_activation}}_str,
cast(NULL as int64) as month_diff_int,
cast(NULL as float64) as total_amount,
cast(NULL as float64) as cumulative_amount,
count(distinct person_id) as donors_in_cohort
from {{ ref(first_gift_table)}}
where first_gift_recur_status = {{boolean_status}}
group by 1, 2, 3, 4, 5, 6, 7, 8

)

, rev_by_cohort as (
    select 
        join_month_year_str,
        first_gift_join_source,
        join_gift_size_string{{recur_suffix}},
        first_gift_donor_audience,
        concat('{{ ret_or_act }}' || month_diff_str ) as {{retention_or_activation}}_str,
        month_diff_int,
        sum(amounts) as total_amount
    from {{ ref(transactions_table) }} 
    where first_gift_recur_status = {{boolean_status}}
    group by 1, 2, 3, 4, 5, 6
),

add_cumulative as (
    select 
        join_month_year_str,
        first_gift_join_source,
        join_gift_size_string{{recur_suffix}},
        first_gift_donor_audience,
        {{retention_or_activation}}_str,
        month_diff_int,
        total_amount,
        SUM(total_amount) OVER (
            PARTITION BY join_month_year_str, first_gift_join_source, join_gift_size_string{{recur_suffix}}, first_gift_donor_audience
            ORDER BY month_diff_int asc
        ) AS cumulative_amount
    from rev_by_cohort
)

    select 
        coalesce(add_cumulative.join_month_year_str, first_gift_rollup.join_month_year_str) as join_month_year_str,
        coalesce(add_cumulative.first_gift_join_source, first_gift_rollup.first_gift_join_source) as join_source,
        coalesce(add_cumulative.join_gift_size_string{{recur_suffix}}, first_gift_rollup.join_gift_size_string{{recur_suffix}}) as join_gift_size,
        coalesce(add_cumulative.first_gift_donor_audience, first_gift_rollup.first_gift_donor_audience) as join_donor_audience,
        coalesce(add_cumulative.{{retention_or_activation}}_str, first_gift_rollup.{{retention_or_activation}}_str) as {{retention_or_activation}}_str,
        coalesce(add_cumulative.month_diff_int, first_gift_rollup.month_diff_int) as {{retention_or_activation}}_int,
        coalesce(add_cumulative.total_amount, first_gift_rollup.total_amount) as total_amount,
        coalesce(add_cumulative.cumulative_amount, first_gift_rollup.cumulative_amount) as cumulative_amount,
        first_gift_rollup.donors_in_cohort
    from add_cumulative
    full outer join first_gift_rollup
    on add_cumulative.join_month_year_str = first_gift_rollup.join_month_year_str
    and add_cumulative.first_gift_join_source = first_gift_rollup.first_gift_join_source
    and add_cumulative.join_gift_size_string{{recur_suffix}} = first_gift_rollup.join_gift_size_string{{recur_suffix}}
    and add_cumulative.first_gift_donor_audience = first_gift_rollup.first_gift_donor_audience


{% endmacro %}
