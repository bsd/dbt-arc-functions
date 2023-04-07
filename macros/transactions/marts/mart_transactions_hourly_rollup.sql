{% macro create_mart_transactions_hourly_rollup(
    reference_name="stg_transactions_unioned"
) %}
select
    best_guess_message_id,
    datetime_trunc(transaction_timestamp, hour) as hour_timestamp,
    transaction_source_code,
    channel as channel_from_crm,
    channel_from_source_code,
    coalesce(channel, channel_from_source_code) as channel_best_guess,
    campaign,
    audience,
    crm_entity,
    source_code_entity,
    case
        when (crm_entity is not null and source_code_entity is not null)
        then concat(crm_entity, '-', source_code_entity)
        else coalesce(crm_entity, source_code_entity)
    end as best_guess_entity,
    sum(amount) as total_amount,
    sum(case when recurring_revenue then amount else 0 end) as total_recurring_revenue,
    sum(
        case when new_recurring_revenue then amount else 0 end
    ) as total_new_recurring_revenue,
    count(distinct person_id) as number_of_donors,
    count(transaction_id_in_source_crm) as number_of_transactions
from {{ ref(reference_name) }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
{% endmacro %}
