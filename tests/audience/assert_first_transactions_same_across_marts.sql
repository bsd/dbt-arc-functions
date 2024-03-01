
with arc_person as (
select 
    concat(person_id,first_transaction_date) as field_1
from {{ref("stg_stitch_sfmc_arc_person")}}
),

first_gift as (
select 
    concat(person_id,first_transaction_date) as field_2
from {{ref("stg_stitch_sfmc_parameterized_audience_transaction_first_gift")}}
),

first_issue as (
    select field_1 from arc_person 
    where field_1 not in (select field_2 from first_gift)
),

second_issue as (
    select field_2 from first_gift 
    where field_2 not in (select field_1 from arc_person)
),

unioned_issues as (
    select * from first_issue
    union all
    select * from second_issue
)

select * from unioned_issues

