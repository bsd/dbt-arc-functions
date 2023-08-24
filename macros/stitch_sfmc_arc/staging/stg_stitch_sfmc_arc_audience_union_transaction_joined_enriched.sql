{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined",
    audience_calculated_alldates ="stg_stitch_sfmc_audience_transaction_calculated_alldates"
) %}

select
    audience_union_transaction_joined.transaction_date_day,
    audience_union_transaction_joined.person_id,
    audience_union_transaction_joined.donor_audience as audience_unioned,
    audience_calculated_alldates.donor_audience as audience_calculated,
    donor_engagement,
    channel_category,
    inbound_channel,
    gift_size_string,
    recurring,
    amount,
    1 as gift_count,
    coalesce(
        audience_union_transaction_joined.donor_audience,
        audience_calculated_alldates.donor_audience
    ) as coalesced_audience,
    case
        when
            audience_union_transaction_joined.donor_audience is not null
            then 'audience_union_transaction_joined.donor_audience'
        else 'audience_calculated_alldates.donor_audience'
    end as source_column
from
    {{ ref('stg_stitch_sfmc_arc_audience_union_transaction_joined') }}
        as audience_union_transaction_joined
left join
    {{ ref('stg_stitch_sfmc_audience_transaction_calculated_alldates') }}
        as audience_calculated_alldates
    on
        audience_calculated_alldates.transaction_date_day
        = audience_union_transaction_joined.transaction_date_day
        and audience_calculated_alldates.person_id
        = audience_union_transaction_joined.person_id


{% endmacro %}