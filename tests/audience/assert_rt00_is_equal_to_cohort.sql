with rt100 as (
    select 
        donors_in_cohort,
        donors_retained
    from {{ref(mart_audience_recur_retention)}}
    where retention_int = 0
)

select * from rt100
where donors_in_cohort != donors_retained


