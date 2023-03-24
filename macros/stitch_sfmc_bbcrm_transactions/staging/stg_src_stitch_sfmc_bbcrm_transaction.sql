{% macro create_stg_src_stitch_sfmc_bbcrm_transaction() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^revenue$',
    is_source=True,
  source_name='src_stitch_bbcrm',
  schema_to_search='src_stitch_bbcrm_authorized') %}

with revenue as (
Select DISTINCT
            __bbcrmlookupid_ as bbcrmlookupid,
            constituentsystemrecordid as constituentsystemrecordid,
            SAFE_CAST(statuscode as string) as statuscode,
            SAFE_CAST(recordid as string) as recordid,
            revenue_id as revenue_id,
            SAFE_CAST(transaction_date as datetime) as transaction_date,
            payment_method as payment_method,
            SAFE_CAST(recognition_amount as int64) as amount,
            inbound_channel as inbound_channel,
            appeal as appeal,
            appeal_business_unit as appeal_business_unit,
            initial_market_source as initial_market_source,
            web_donation_form_value as web_donation_form_value,
            web_donation_form_comment as web_donation_form_comment,
            designation as designation,
            transaction_type as transaction_type,
            application as application,
            vendor_order_number as vendor_order_number,
            revenue_platform as revenue_platform,
            SAFE_CAST(sfmc_dateadded as datetime) as sfmc_dateadded,
            SAFE_CAST(sfmc_updatedate as datetime) as sfmc_updatedate

        from ({{ dbt_utils.union_relations(relations) }})

), current_fiscal_ranked as (

        select
            *,
            row_number() over (
                partition by revenue_id order by sfmc_updatedate desc
            ) as row_num
        from revenue

    ),
    final as (select * except (row_num) from current_fiscal_ranked where row_num = 1 AND revenue_id != 'rev-44816929')
select *
from final
where cast(transaction_date as datetime) < current_datetime()

{% endmacro %}

