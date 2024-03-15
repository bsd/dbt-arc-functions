with
transactions as (
    select
        *
    from {{ref('stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned')}}
),

audience as (select * from {{ref('stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched')}})

select is_digital, count(*) from transactions
where transaction_id not in (select distinct transaction_id from audience)
group by 1