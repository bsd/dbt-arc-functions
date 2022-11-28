{% macro create_stg_paidmedia_actuals_goals_join_transform(
    reference_name='stg_paidmedia_actuals_goals_join') %}
SELECT
     month_year,
     case when regexp_contains(channel, 'soc') = true then 'Social'
     case when regexp_contains(platform, 'soc') = true then 'Social'
     when regexp_contains(channel, 'sear') = true then 'Search'
     when regexp_contains(platform, 'sear') = true then 'Search'
     when regexp_contains(channel, 'disp') = true then 'Display'
     when regexp_contains(platform, 'disp') = true then 'Display'
     when regexp_contains(channel, 'vid') = true then 'Display'
     when regexp_contains(platform, 'vid') = true then 'Display'
     when regexp_contains(channel, 'youtube') = true then 'Display'
     when regexp_contains(platform, 'youtube') = true then 'Display'
     else channel end as channel,
    case when regexp_contains(objective, 'awar') = true then 'Awareness'
    when regexp_contains(objective, 'acq') = true then 'Acquisition'
    when regexp_contains(objective, 'fun') = true then 'Fundraising'
    else objective end as objective,
      case when regexp_contains(platform, 'goog') = true then 'Google'
      case when regexp_contains(platform, 'bing') = true then 'Bing'
      case when regexp_contains(platform, 'facebook') = true then 'Facebook'
      case when regexp_contains(platform, 'fb') = true then 'Facebook'
      case when regexp_contains(platform, 'instagram') = true then 'Facebook'
      case when regexp_contains(platform, 'youtube') = true then 'Google'
      case when regexp_contains(platform, 'yt') = true then 'Google'
      case when regexp_contains(platform, 'linked') = true then 'LinkedIn'
      case when regexp_contains(platform, 'yahoo') = true then 'Yahoo'
        else platform end as platform,
      sum(actual_spend) as actual_spend,
      sum(actual_revenue) as actual_revenue,
      sum(actual_donations) as actual_donations
      sum(projected_revenue) as projected_revenue
      sum(projected_spend) as projected_spend
FROM {{ ref(reference_name) }} 
GROUP BY 1, 2, 3, 4
{% endmacro %}