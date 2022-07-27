# dbt ARC Functions

Functions used across clients using dbt in Analytics Reporting Cloud.

## Currently available products

Look [here in macros](https://github.com/bsd/dbt-arc-functions/tree/main/macros) for products we currently support.

Look [here in utils](https://github.com/bsd/dbt-arc-functions/tree/main/utils) for utilities that will help you get a dbt project going faster!

## How to Submit Bugs

* usually, bug is identified by blue state client or analyst on account

* bug is confirmed to be a result of the dbt-arc-functions by engineer

* bug report created in (ARC Jira project)[https://bluestate.atlassian.net/jira/software/c/projects/ARC/boards/245?selectedIssue=ARC-753&quickFilter=580]

* create bug report as an issue in the GitHub project, adding a) what you expect to see, b) what you actually see, c) include screenshots and links + logging wherever possible


## How to Contribute

1. Claim Bug from the issue list

2. Create new branch

3. Make changes and test them, check the boxes below:

[]- Test that the project still successfully dbt compile 

[] Test branch on a development branch of an existing client (ideally the one that raised the bug)

[] do this by changing revision of the package to the branch name in packages.yml file in dbt

[] also re-run the `create_or_update_standard_models.py` for the client dbt project, replacing models and dependencies

[]check that client package successfully runs dbt deps , dbt compile , and dbt run; screenshot this page

[] query the resulting staging model in bigquery and check that the bug is resolved; screenshot this

[] Add testing requirements language and screenshots to PR

[] Link bug issue to PR

4. Ask for PR review from teammates: Reviewers should review code, and run the above tests themselves, then approve
