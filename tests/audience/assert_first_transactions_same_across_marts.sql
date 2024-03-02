{{ config(severity="warn") }}

with
    arc_person as (
        select
            person_id,
            first_transaction_date,
            concat(person_id, first_transaction_date) as field_1
        from {{ ref("stg_stitch_sfmc_arc_person") }}

    ),

    first_gift as (
        select
            person_id,
            first_transaction_date,
            concat(person_id, first_transaction_date) as field_2
        from {{ ref("stg_stitch_sfmc_parameterized_audience_transaction_first_gift") }}
    ),

    first_issue as (
        select distinct field_1
        from arc_person
        where field_1 not in (select distinct field_2 from first_gift)
    ),

    second_issue as (
        select distinct field_2
        from first_gift
        where field_2 not in (select distinct field_1 from arc_person)
    ),

    third_issue as (
        select distinct person_id
        from arc_person
        where person_id not in (select distinct person_id from first_gift)
    ),

    fourth_issue as (
        select distinct person_id
        from first_gift
        where person_id not in (select distinct person_id from arc_person)

    ),

    join_by_person as (
        select
            first_gift.person_id,
            first_gift.first_transaction_date as first_gift_table_date,
            arc_person.first_transaction_date as arc_person_table_date
        from first_gift
        inner join arc_person using (person_id)
    ),

    fifth_issue as (
        select
            concat(
                person_id,
                'first_gift',
                first_gift_table_date,
                'arc_person',
                arc_person_table_date
            ) as issue
        from join_by_person
        where first_gift_table_date != arc_person_table_date

    ),

    unioned_issues as (
        select 'fields_in_arc_person_not_first_gift' as source_table, field_1 as issue
        from first_issue
        union all
        select 'fields_in_first_gift_not_arc_person' as source_table, field_2 as issue
        from second_issue
        union all
        select
            'person_id_in_arc_person_not_first_gift' as source_table, person_id as issue
        from third_issue
        union all
        select
            'person_id_in_first_gift_not_arc_person' as source_table, person_id as issue
        from fourth_issue
        union all
        select 'first_dates_dont_match_up_for_person' as source_table, issue
        from fifth_issue
    )

select source_table, count(*)
from unioned_issues
group by 1
