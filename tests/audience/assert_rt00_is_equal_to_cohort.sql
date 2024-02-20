with
    rt00 as (
        select * from {{ ref("mart_audience_recur_retention") }} where retention_int = 0
    )

select *
from rt00
where donors_in_cohort != donors_retained
