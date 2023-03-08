{% macro create_stg_src_stitch_sfmc_recent_transactions() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^recent_transactions$',
    is_source=True,
  source_name='stitch_sfmc_frundraiseup',
  schema_to_search='src_stitch_fundraiseup_authorized') %}

SELECT Distinct
        fru_donation_id as revenue_id
        ,__initial_market_source_ as initial_market_source
        ,email_address as email
        ,CAST(amount as FLOAT64) amount
        ,gift_type
        ,CAST(transaction_date AS DATETIME) as transaction_date
        ,appeal
        ,designation
        ,CAST(SUBSTR(sfmc_updatedate,1,19) AS DATETIME) as sfmc_insert_dt  --dummy field since this is missing from the staging table
        ,CAST(migrated AS BOOL) migrated
        ,bbcrmlookupid as lookup_id
        ,CAST(SUBSTR(sfmc_updatedate,1,19) AS DATETIME) as sfmc_updated_dt
        ,_sdc_received_at

    from ({{ dbt_utils.union_relations(relations) }})


{% endmacro %}