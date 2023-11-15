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
count(distinct person_id) as donors_in_cohort
from {{ ref(first_gift_table)}}
where first_gift_recur_status = {{boolean_status}}
group by 1, 2, 3, 4

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
    where first_gift_recur_status = '{{ recur_status }}'
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
            PARTITION BY join_month_year_str, first_gift_join_source, join_gift_size_string{{recur_suffix}}, first_gift_donor_audience, {{retention_or_activation}}_str
            ORDER BY month_diff_int
        ) AS cumulative_amount
    from rev_by_cohort
),

donors_in_cohort as (
    select 
        add_cumulative.*,
        first_gift_rollup.donors_in_cohort
    from add_cumulative
    left join first_gift_rollup
    on add_cumulative.join_month_year_str = first_gift_rollup.join_month_year_str
    and add_cumulative.first_gift_join_source = first_gift_rollup.first_gift_join_source
    and add_cumulative.join_gift_size_string{{recur_suffix}} = first_gift_rollup.join_gift_size_string{{recur_suffix}}
    and add_cumulative.first_gift_donor_audience = first_gift_rollup.first_gift_donor_audience
)

select
    join_month_year_str as join_month_year,
    first_gift_join_source as join_source,
    join_gift_size_string{{recur_suffix}} as join_gift_size,
    first_gift_donor_audience as join_donor_audience,
    {{retention_or_activation}}_str,
    total_amount,
    cumulative_amount,
    donors_in_cohort
from donors_in_cohort

{% endmacro %}
