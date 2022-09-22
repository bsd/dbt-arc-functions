{% docs mart_email_listsize %}

## model: mart_email_list_size_by_month_date
### description: a model that pulls in max sends and max delivered by month from mart_email_performance_with_revenue
#### columns:
  - **month_date**: first day of the actual month and actual year the email with max recipients was sent
  - **max_recipients**: email with largest number of recipients for that month
  - **max_delivered**: email with largest number of delivered for that month

{% enddocs %}