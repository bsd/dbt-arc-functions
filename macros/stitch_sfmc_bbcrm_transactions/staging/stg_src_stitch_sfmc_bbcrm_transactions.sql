{% macro create_stg_src_stitch_sfmc_bbcrm_recent_transactions() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^bbcrm_revenue$',
    is_source=True,
  source_name='src_stitch_bbcrm',
  schema_to_search='src_stitch_bbcrm_authorized') %}

Select DISTINCT
            __bbcrmlookupid_ as bbcrmlookupid,
            constituentsystemrecordid as constituentsystemrecordid,
            cast(statuscode as string) as statuscode,
            cast(recordid as string) as recordid,
            revenue_id as revenue_id,
            transaction_date as transaction_date,
            payment_method as payment_method,
            cast(recognition_amount as string) as recognition_amount,
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
            sfmc_dateadded as sfmc_dateadded,
            sfmc_updatedate as sfmc_updatedate

        from ({{ dbt_utils.union_relations(relations) }})


{% endmacro %}

