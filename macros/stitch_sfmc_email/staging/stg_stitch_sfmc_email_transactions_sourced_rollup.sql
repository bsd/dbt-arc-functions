{% macro create_stg_stitch_sfmc_email_transactions_sourced_rollup(
    email_summary="stg_stitch_sfmc_email_jobs",
    transactions="stg_stitch_sfmc_transactions_unioned"
) %}

with
    grouped as (
        select
            email_summary.message_id as message_id,
            sum(safe_cast(transactions.amount as numeric)) as total_revenue,
            count(distinct transactions.transaction_id) as total_gifts,
            count(distinct transactions.person_id) as total_donors,
            sum(
                case when transactions.recurring = 'false' then transactions.amount end
            ) as one_time_revenue,
            count(
                case
                    when transactions.recurring = 'false'
                    then transactions.transaction_id
                end
            ) as one_time_gifts, 
            --sum(case when transactions.recurring_number = 1 then transactions.amount end) 
            null as new_monthly_revenue,
            --count(case when transactions.recurring_number = 1 then transactions.transaction_id end) 
            null as new_monthly_gifts,
            sum(
                case when transactions.recurring = 'true' then transactions.amount end
            ) as total_monthly_revenue,
            count(
                case
                    when transactions.recurring = 'true'
                    then transactions.transaction_id
                end
            ) as total_monthly_gifts
        from {{ ref(email_summary) }} email_summary
        left join
            {{ ref(transactions) }} transactions
            on cast(transactions.message_id as string) = cast(email_summary.message_id as string)
        group by 1
    )

select
    safe_cast(message_id as string) as message_id,
    safe_cast(total_revenue as numeric) as total_revenue,
    safe_cast(total_gifts as int) as total_gifts,
    safe_cast(total_donors as int) as total_donors,
    safe_cast(one_time_gifts as int) as one_time_gifts,
    safe_cast(one_time_revenue as numeric) as one_time_revenue,
    safe_cast(new_monthly_revenue as numeric) as new_monthly_revenue,
    safe_cast(new_monthly_gifts as int) as new_monthly_gifts,
    safe_cast(total_monthly_revenue as numeric) as total_monthly_revenue,
    safe_cast(total_monthly_gifts as int) as total_monthly_gifts
from grouped

{% endmacro %}
