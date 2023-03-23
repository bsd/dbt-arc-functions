{% macro create_stg_src_stitch_sfmc_fundraiseup_recent_transactions() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^recent_transactions$',
    is_source=True,
  source_name='src_stitch_bbcrm',
  schema_to_search='src_stitch_bbcrm_authorized') %}

with fru as (
    SELECT
        fru_donation_id as revenue_id
        ,SAFE_CAST(__initial_market_source_ as STRING) as initial_market_source
        ,email_address as email
        ,SAFE_CAST(amount as FLOAT64) amount
        ,gift_type
        ,SAFE_CAST(transaction_date AS DATETIME) as transaction_date
        ,appeal
        ,designation
        ,SAFE_CAST(SUBSTR(sfmc_updatedate,1,19) AS DATETIME) as sfmc_insert_dt
        ,SAFE_CAST(migrated AS BOOL) migrated
        ,bbcrmlookupid as lookup_id
        ,SAFE_CAST(SUBSTR(sfmc_updatedate,1,19) AS DATETIME) as sfmc_updated_dt
        ,SAFE_CAST(SUBSTR(_sdc_received_at,1,19) AS DATETIME) as _sdc_received_at

    from  {{ dbt_utils.union_relations(relations) }}

), fru_ranked as (

    SELECT *
    ,row_number() OVER (Partition by revenue_id order by transaction_date desc, _sdc_received_at desc nulls last) as row_num
    FROM fru
)
Select
* Except(row_num, _sdc_received_at)
from fru_ranked WHERE row_num = 1
AND CAST(amount AS FLOAT64) < 9999
{% endmacro %}