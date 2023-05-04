{% macro create_stg_frakture_everyaction_email_transactions_sourced_rollup(
    email_summary="stg_frakture_everyaction_email_summary_unioned",
    transactions="stg_frakture_everyaction_transactions_summary_unioned"
) %}

{% if var.database == 'bsd-vera'%}

 with
        grouped as (
            select
                email_summary.message_id as message_id,
                sum(safe_cast(transactions.amount as numeric)) as total_revenue,
                count(distinct transactions.remote_transaction_id) as total_gifts,
                count(distinct transactions.remote_person_id) as total_donors,
                sum(
                    case when transactions.recurs is null then transactions.amount end
                ) as one_time_revenue,
                count(
                    case
                        when transactions.recurs is null
                        then transactions.remote_transaction_id
                    end
                ) as one_time_gifts,
                sum(
                    case
                        when transactions.recurring_number = 1 then transactions.amount
                    end
                ) as new_monthly_revenue,
                count(
                    case
                        when transactions.recurring_number = 1
                        then transactions.remote_transaction_id
                    end
                ) as new_monthly_gifts,
                sum(
                    case
                        when transactions.recurs = 'monthly' then transactions.amount
                    end
                ) as total_monthly_revenue,
                count(
                    case
                        when transactions.recurs = 'monthly'
                        then transactions.remote_transaction_id
                    end
                ) as total_monthly_gifts
            from {{ ref(email_summary) }} email_summary
            full outer join
                {{ ref(transactions) }} transactions
                on regexp_replace(cast(email_summary.final_primary_source_code as string), '(hdr|ftr)$', '') =
                regexp_replace(cast(transactions.source_code as string), '(hdr|ftr)$', '')
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


{% else %}

    with
        grouped as (
            select
                email_summary.message_id as message_id,
                sum(safe_cast(transactions.amount as numeric)) as total_revenue,
                count(distinct transactions.remote_transaction_id) as total_gifts,
                count(distinct transactions.remote_person_id) as total_donors,
                sum(
                    case when transactions.recurs is null then transactions.amount end
                ) as one_time_revenue,
                count(
                    case
                        when transactions.recurs is null
                        then transactions.remote_transaction_id
                    end
                ) as one_time_gifts,
                sum(
                    case
                        when transactions.recurring_number = 1 then transactions.amount
                    end
                ) as new_monthly_revenue,
                count(
                    case
                        when transactions.recurring_number = 1
                        then transactions.remote_transaction_id
                    end
                ) as new_monthly_gifts,
                sum(
                    case
                        when transactions.recurs = 'monthly' then transactions.amount
                    end
                ) as total_monthly_revenue,
                count(
                    case
                        when transactions.recurs = 'monthly'
                        then transactions.remote_transaction_id
                    end
                ) as total_monthly_gifts
            from {{ ref(email_summary) }} email_summary
            full outer join
                {{ ref(transactions) }} transactions
                on cast(email_summary.final_primary_source_code as string)
                = cast(transactions.source_code as string)
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

{% endif %}
{% endmacro %}
