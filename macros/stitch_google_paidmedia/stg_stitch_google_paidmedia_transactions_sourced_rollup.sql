   select
        id as message_id,
        date as date_timestamp,
        cast(null as int) as total_revenue,
        cast(null as int) as total_gifts,
        cast(null as int) as total_donors,
        cast(null as int) as one_time_revenue,
        cast(null as int) as one_time_gifts,
        cast(null as int) as new_monthly_revenue,
        cast(null as int) as new_monthly_gifts,
        cast(null as int) as total_monthly_revenue,
        cast(null as int) as total_monthly_gifts,
        case when campaign_name like '%Appeal%' then 'donate'
        when campaign_name like '%Brand%' then 'brand'
        else null end as objective,
        campaign_name as campaign,
        cast(null as string) as campaign_label,
        cast(null as string) as audience,
        cast(null as string) as appeal,
        cast(null as string) as source_code,
 from  {{ source("src_stitch_googleads_paidmedia", "google_ad_performance" ) }}