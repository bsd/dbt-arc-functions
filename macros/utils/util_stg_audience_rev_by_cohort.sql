-- fmt: off
{% macro util_stg_audience_transaction_rev_by_cohort(
    recur_status,
    transactions_table="stg_audience_transaction_with_first_gift_cohort"
) %}

    {% if recur_status not in ["recurring", "onetime"] %}
        {{
            exceptions.raise_compiler_error(
                "'recur_status' argument to util_stg_stitch_sfmc_audience_transaction_rev_by_cohort must be 'recurring' or 'onetime', got "
                ~ recur_status
            )
        }}
    {% endif %}

    {% set recur_suffix = "_recur" if recur_status == "recurring" else "" %}
    {% set boolean_status = True if recur_status == "recurring" else False %}

    select
        join_month_year_str,
        coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
        join_gift_size_string{{ recur_suffix }},
        first_gift_donor_audience,
        month_diff_int,
        sum(
            case when recurring = {{ boolean_status }} then 1 else 0 end
        ) as unique_donations,
        sum(amounts) as total_amount
    from {{ ref(transactions_table) }}
    where first_gift_recur_status = {{ boolean_status }}
    group by 1, 2, 3, 4, 5

{% endmacro %}
