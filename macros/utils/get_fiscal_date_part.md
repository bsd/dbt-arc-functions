{% docs get_fiscal_date_part %}
{% raw %}
      A macro that allows you to get fiscal day from a date, datetime,
      or timestamp with a fiscal year start date. Usage shown below:

```sql   
SELECT date_timestamp,
  channel,
  best_guess_entity,
  {{dbt_arc_functions.get_fiscal_year(
      'date_timestamp', 
      var('fiscal_year_start'))}}
    AS fiscal_year,
  {{dbt_arc_functions.get_fiscal_quarter(
      'date_timestamp',
      var('fiscal_year_start'))}}
    AS fiscal_quarter,
  {{dbt_arc_functions.get_fiscal_month(
      'date_timestamp',
      var('fiscal_year_start'))}}
    AS fiscal_month,
  {{dbt_arc_functions.get_fiscal_week(
      'date_timestamp',
      var('fiscal_year_start'))}}
    AS fiscal_week,
  {{dbt_arc_functions.get_fiscal_day(
      'date_timestamp',
      var('fiscal_year_start'))}}
    AS fiscal_day,
  total_gifts,
  SUM(total_gifts)
    OVER(PARTITION BY channel, best_guess_entity, 
    {{dbt_arc_functions.get_fiscal_year(
        'date_timestamp',
        var('fiscal_year_start'))}}
    ORDER BY date_timestamp) 
    AS total_gifts_cumulative_fiscal_year,
  total_revenue,
  SUM(total_revenue)
    OVER(PARTITION BY channel, best_guess_entity,
    {{dbt_arc_functions.get_fiscal_year(
        'date_timestamp',
        var('fiscal_year_start'))}}
    ORDER BY date_timestamp) 
    AS total_revenue_cumulative_fiscal_year,
FROM {{ ref('YOUR_MODEL_HERE') }}
WHERE total_gifts > 0 OR total_revenue > 0
ORDER BY 2,3,1 ASC
```
{% endraw %}
{% enddocs %}