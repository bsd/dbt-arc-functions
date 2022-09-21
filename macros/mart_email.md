{% docs mart_email %}

## model: mart_email_performance_with_revenue
### description: a model that rolls up email performance KPIs by email name, showing a date
#### columns:
  - **message_id**: frakture ID that is unique to each creative, but not unique to each row
  - **from_name**: the name of the person or entity that sent the email
  - **from_email**: the name of the email address that sent the email
  - **best_guess_timestamp**: the best guess timestamp for a sent date (use this one)        
  - **scheduled_timestamp**: the time an email was scheduled to be sent
  - **pickup_timestamp**
  - **delivered_timestamp** : the time an email was delivered
  - **campaign_start_timestamp**: the timestamp of the earliest email sent in a campaign      
  - **campaign_latest_timestamp**: the timestamp of the latest email sent in a campaign
  - **email_name**: label name of the email
  - **email_subject**: email subject line
  - **source_code**: email source code       
  - **crm_entity**: account name, for example facebook 1, facebook 2
  - **source_code_entity**: parsed segment of frakture source code field "entity"
  - **best_guess_entity**: concatenation of crm_entity and source_code entity
  - **audience**: parsed segment of frakture source code field "audience"
  - **campaign_category**: parsed segment of frakture source code field "message_set"
  - **campaign_name**: coalesce of campaign name field from CRM or parsed segment of frakture source code field "campaign"
  - **recurtype**: parsed segment of frakture source code field "recurtype"; a flag for triggered vs bulk email
  - **recipients**: count of email total sends        
  - **opens**: count of email total opens
  - **clicks**: count of email total clicks
  - **actions**:
      - Actionkit: count of email total actions, ,or count of entries in core_action table minus donations, unsubscribe, and incompletes
      - in EveryAction: FormSubmissionCount 
      - in EngagingNetworks: Advocacy Count
  - **total_bounces**: count of total bounces        
  - **block_bounces**
  - **tech_bounces**
  - **soft_bounces**
  - **unknown_bounces**
  - **unsubscribes**: count of unsubscribes
  - **total_revenue**: sum of total attributed revenue
  - **total_gifts**: count of total attributed transactions
  - **total_donors**: count of unique donors using origin person ID    
  - **one_time_revenue**: sum of one time attributed revenue
  - **one_time_gifts**: count of one time attributed transactions
  - **new_monthly_revenue**: sum of initial attributed recurring revenue
  - **new_monthly_gifts**: count of intial attributed recurring transactions
  - **total_monthly_revenue**: sum of total attributed recurring revenue
  - **total_monthly_gifts**: count of total attributed recurring transactions

{% enddocs %}
