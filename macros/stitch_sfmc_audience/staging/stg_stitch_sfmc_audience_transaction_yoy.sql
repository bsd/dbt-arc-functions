{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name = 'stg_src_stitch_sfmc_audience_transactions_unioned'
) %}

SELECT 
person_id,
  MAX(CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), transaction_date, YEAR) = 0 THEN 1
        ELSE 0
      END) AS donated_this_year,
  MAX(CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), transaction_date, YEAR) = 1 THEN 1
        ELSE 0
      END) AS donated_last_year,
  MAX(CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), transaction_date, YEAR) = 2 THEN 1
        ELSE 0
      END) AS donated_two_years_ago,
  MAX(CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), transaction_date, YEAR) = 3 THEN 1
        ELSE 0
      END) AS donated_three_years_ago

FROM {{ ref(reference_name) }}
GROUP BY person_id

{% endmacro %}


  
