{% macro create_stg_frakture_everyaction_transactions() %}
  {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^everyaction_[A-Za-z0-9]{3}_transaction$",
        is_source=True,
        source_name="frakture_everyaction_transactions",
        schema_to_search="src_frakture",
    ) %}
    select
        'everyaction' as source_crm,
        remote_transaction_id as transaction_id_in_source_crm,
        remote_person_id as person_id,
        amount - refund_amount as amount,
        safe_cast(
            {{ dbt_date.convert_timezone("cast(ts as TIMESTAMP)") }} as timestamp
        ) as transaction_timestamp,
        case when recurs = 'monthly' then 1 else 0 end as recurring_revenue,
        safe_cast(null as int) as new_recurring_revenue,
        coalesce(everyaction_market_source,source_code) as transaction_source_code,
        safe_cast(null as string) as best_guess_message_id,
        safe_cast(null as string) as campaign,
        'email' as channel,
        safe_cast(null as string) as audience,
        'everyaction' as crm_entity,
        safe_cast(null as string) as source_code_entity,
        safe_cast(null as string) as channel_from_source_code
    from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
