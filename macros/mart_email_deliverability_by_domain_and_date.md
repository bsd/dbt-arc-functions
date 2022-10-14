{% docs mart_email_deliverability_by_domain_and_date %}

## model: mart_email_deliverability_by_domain_and_date
### description: a model that pulls in person table data and event stream data (in the case of frakture, tables that have a suffix of per_person_message_stat, which is not available yet for every CRM). It displays deliverability KPIs per domain per email message ID per date.
#### columns:
  - **sent_date**: date the corresponding email message was sent
  - **message_id**: frakture unique ID for email messages
  - **email_domain**: email domain of the corresponding person, from their person table
  - **domain_category**: custom categorization of email domains into simpler names
  - **recipients**: # of email recipients reported in the timeline
  - **opens**: # of email opens reported in the timeline; excludes machine opens in EA
  - **clicks**: # of email clicks reported in the timeline
  - **actions**: # of advocacy actions (often including signups and petition signatures) reported in the timeline
  - **total_bounces**: soft_bounces + hard_bounces
  - **soft_bounces**: # of temporary bounces
  - **hard_bounces**: # of hard bounces that are never delivered
  - **unsubscribes**: # of unsubscrubes
{% enddocs %}