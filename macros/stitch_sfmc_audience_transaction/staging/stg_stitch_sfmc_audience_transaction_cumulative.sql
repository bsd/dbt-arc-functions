{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

SELECT 
    transaction_date,
    person_id,
    -- total amount and gifts
    SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS cumulative_amount_30_days,
    COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS cumulative_gifts_30_days,
    SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_amount_6_months,
    COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_gifts_6_months,
    SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_amount_12_months,
    COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_gifts_12_months,
    SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_amount_24_months,
    COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) AS cumulative_gifts_24_months, 
    -- only recurring gifts
    -- total amount and gifts
    CASE when recurring = 1 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_30_days_recur,
    case when recurring = 1 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_30_days_recur,
    case when recurring = 1 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_6_months_recur,
    case when recurring = 1 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_6_months_recur,
    case when recurring = 1 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_12_months_recur,
    case when recurring = 1 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_12_months_recur,
    case when recurring = 1 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_24_months_recur,
    case when recurring = 1 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_24_months_recur,
    -- only one time gifts
    CASE when recurring = 0 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_30_days_onetime,
    case when recurring = 0 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_30_days_onetime,
    case when recurring = 0 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_6_months_onetime,
    case when recurring = 0 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 6 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_6_months_onetime,
    case when recurring = 0 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_12_months_onetime,
    case when recurring = 0 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 12 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_12_months_onetime,
    case when recurring = 0 then SUM(amount) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_amount_24_months_onetime,
    case when recurring = 0 then COUNT(*) OVER (
        PARTITION BY person_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL 24 MONTH PRECEDING AND CURRENT ROW
    ) else 0 end AS cumulative_gifts_24_months_onetime

FROM {{ ref(reference_name) }}



{% endmacro %}
