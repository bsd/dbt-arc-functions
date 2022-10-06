{% docs mart_email_listhealth %}

## model: mart_email_listhealth_by_month
### description: a model that pulls in aggregate list stats from mart_email_performance_with_revenue by month and also pulls in previous month's data in order to create a running "diff"
#### columns:
  - **month_date**: first day of the actual month and actual year the email with max recipients was sent
  - **max_recipients**: email with largest number of recipients for that month
  - **max_delivered**: email with largest number of delivered for that month
  - **delivered_prev_month**: previous month's figure for max_delivered
  - **diff_delivered_prev_month**: (current month - previous month) for max_delivered
  - **total_unsubscribes**: sum of unsubscribes for month
  - **total_hard_bounces**: sum of hard_bounces specifically for month
  - **total_complaints**: sum of complaints for month

{% enddocs %}