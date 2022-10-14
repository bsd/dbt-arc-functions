{% docs mart_combined_email_paidmedia_daily_revenue_performance %}

## model: mart_combined_email_paidmedia_daily_revenue_performance
### description: a model that unions the mart_paidmedia model and the mart_email model
#### columns:
  - **channel**: bot name from frakture
  - **best_guess_entity**:
      concatenates the bot label and the entity-related source code together. 
      aim is to identify multiple accounts from the same channel.
  - **total_revenue**: sum of all revenue
  - **total_gifts**: count of transactions

{% enddocs %}