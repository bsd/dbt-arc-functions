{% docs mart_email_deliverability_by_domain_and_date %}

## model: mart_email_deliverability_by_domain_and_date
### description: a model that pulls in person table data and event stream data (in the case of frakture, tables that have a suffix of per_person_message_stat, which is not available yet for every CRM). It displays deliverability KPIs per domain per email message ID per date.
#### columns:
  - **sent_date**:
  - **message_id**:
  - **email_domain**:
  - **domain_category**:
  - **recipients**:
  - **opens**:
  - **clicks**:
  - **actions**:
  - **total_bounces**: soft_bounces + hard_bounces
  - **soft_bounces**:
  - **hard_bounces**:
  - **unsubscribes**:
{% enddocs %}