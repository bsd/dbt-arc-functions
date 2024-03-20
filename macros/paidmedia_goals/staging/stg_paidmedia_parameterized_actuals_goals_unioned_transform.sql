{% macro create_stg_paidmedia_parameterized_actuals_goals_unioned_transform(
    channel="null",
    objective="null",
    platform="null",
    reference_name="stg_paidmedia_actuals_goals_unioned"
) %}
    select
        month_year,
        parse_date('%B %Y', month_year) as month_year_date,
        -- case # may apply to fracture based clients
        -- when regexp_contains(channel, 'soc') = true
        -- then 'Social'
        -- when regexp_contains(platform, 'soc') = true
        -- then 'Social'
        -- when regexp_contains(channel, 'sear') = true
        -- then 'Search'
        -- when regexp_contains(platform, 'sear') = true
        -- then 'Search'
        -- when regexp_contains(channel, 'disp') = true
        -- then 'Display'
        -- when regexp_contains(platform, 'disp') = true
        -- then 'Display'
        -- when regexp_contains(channel, 'vid') = true
        -- then 'Display'
        -- when regexp_contains(platform, 'vid') = true
        -- then 'Display'
        -- when regexp_contains(channel, 'youtube') = true
        -- then 'Display'
        -- when regexp_contains(platform, 'youtube') = true
        -- then 'Display'
        -- else channel end
        {{ channel }} as channel,
        -- case # may apply to fracture based clients
        -- when regexp_contains(objective, 'awar') = true
        -- then 'Awareness'
        -- when regexp_contains(objective, 'acq') = true
        -- then 'Acquisition'
        -- when regexp_contains(objective, 'fun') = true
        -- then 'Fundraising'
        -- else objective end
        {{ objective }} as objective,
        -- case # may apply to fracture based clients
        -- when regexp_contains(platform, 'goog') = true
        -- then 'Google'
        -- when regexp_contains(platform, 'bing') = true
        -- then 'Bing'
        -- when regexp_contains(platform, 'facebook') = true
        -- then 'Facebook'
        -- when regexp_contains(platform, 'fb') = true
        -- then 'Facebook'
        -- when regexp_contains(platform, 'instagram') = true
        -- then 'Facebook'
        -- when regexp_contains(platform, 'youtube') = true
        -- then 'Google'
        -- when regexp_contains(platform, 'yt') = true
        -- then 'Google'
        -- when regexp_contains(platform, 'linked') = true
        -- then 'LinkedIn'
        -- when regexp_contains(platform, 'yahoo') = true
        -- then 'Yahoo'
        -- when platform = 'ad'
        -- then 'Bing'
        -- else platform end
        {{ platform }} as platform,
        sum(actual_spend) as actual_spend,
        sum(actual_revenue) as actual_revenue, 
        sum(actual_donations) as actual_donations,
        sum(actual_1x_revenue) as actual_1x_revenue,  
        sum(actual_1x_gifts) as actual_1x_gifts,
        sum(actual_monthly_revenue) as actual_monthly_revenue,
        sum(actual_monthly_gifts) as actual_monthly_gifts,
        sum(projected_revenue) as projected_revenue,
        sum(projected_spend) as projected_spend,
        sum(monthly_revenue_target) as monthly_revenue_target,
        sum(monthly_gifts_target) as monthly_gifts_target,
        sum(one_donor_target) as onetime_donor_target,
        sum(one_donor_revenue_target) as onetime_donor_revenue_target,
        sum(total_gifts_target) as total_gifts_target
    from {{ ref(reference_name) }}
    group by 1, 2, 3, 4, 5
{% endmacro %}
