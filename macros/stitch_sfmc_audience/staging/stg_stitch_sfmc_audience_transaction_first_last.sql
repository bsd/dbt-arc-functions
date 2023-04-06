{% macro create_stg_stitch_sfmc_audience_transaction_first_last(
    reference_name = 'stg_src_stitch_sfmc_audience_transactions_unioned'
) %}

SELECT 
transaction_id,
 MAX(transaction_date) OVER (PARTITION BY person_id) AS latest_transaction_date,
  LAG(MAX(transaction_date) OVER (PARTITION BY person_id)) 
  OVER (PARTITION BY person_id ORDER BY transaction_date) 
  AS previous_latest_transaction_date

FROM {{ ref(reference_name) }}

{% endmacro %}


  
