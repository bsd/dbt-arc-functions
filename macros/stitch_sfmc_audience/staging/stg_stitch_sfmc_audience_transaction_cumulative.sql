{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name = 'stg_src_stitch_sfmc_audience_transactions_unioned'
) %}

SELECT 
transaction_id,
SUM(amount) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_amount,
COUNT(*) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_gifts,
SUM(CASE 
  WHEN recurring = 0 THEN amount 
  ELSE 0 
END) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_one_time_amount,
COUNT(CASE 
  WHEN recurring = 0 THEN 1
  ELSE 0 
END) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_one_time_gifts,
SUM(CASE 
  WHEN recurring = 1 THEN amount 
  ELSE 0 
END) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_recur_amount,
COUNT(CASE 
  WHEN recurring = 1 THEN 1
  ELSE 0 
END) OVER (PARTITION BY person_id ORDER BY transaction_date) AS cumulative_recur_gifts

FROM {{ ref(reference_name) }}

{% endmacro %}


  
