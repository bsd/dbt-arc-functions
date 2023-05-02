[![Flake8 Linter](https://github.com/bsd/dbt-arc-functions/actions/workflows/flake8_linter.yml/badge.svg)](https://github.com/bsd/dbt-arc-functions/actions/workflows/flake8_linter.yml)

# dbt ARC Functions

Functions used across clients using dbt in Analytics Reporting Cloud.

Wonder why this repo exists? Look [here](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Data-transformation)!

## Currently available products

Look [here in macros](https://github.com/bsd/dbt-arc-functions/tree/main/macros) for products we currently support.

Look [here in utils](https://github.com/bsd/dbt-arc-functions/tree/main/utils) for utilities that will help you get a dbt project going faster!

## How to Submit Bugs

* usually, bug is identified by blue state client or analyst on account

* bug is confirmed to be a result of the dbt-arc-functions by engineer

* bug report created in [ARC Jira project](https://bluestate.atlassian.net/jira/software/c/projects/ARC/boards/245?selectedIssue=ARC-753&quickFilter=580)

* create bug report as an issue in the GitHub project, adding a) what you expect to see, b) what you actually see, c) include screenshots and links + logging wherever possible


## How to Contribute

1. Claim Bug from the issue list

2. Create new branch

3. Make changes and test them, COPY tests below to PR description tick them:

- [ ] If applicable, add documentation using the `schema.yml` macros documentation with embedded markdown docs (see `mart_email.md` for example) 

- [ ] Test branch on a development branch of an existing client (ideally the one that raised the bug). Do this by changing revision of the package to the branch name in `packages.yml` file in dbt State client below.

- [ ] Re-run `create_or_update_standard_models.py` for the client dbt project, replacing models and dependencies

- [ ] check in dbt that client package successfully runs `dbt deps`, `dbt compile`, and `dbt run`; screenshot this page

- [ ] query the resulting staging model in bigquery (or by previewing mart in dbt cloud); screenshot this

- [ ] If exists, link bug issue and jira tickets to PR

4. Ask for PR review from teammates: Reviewers should review code, and run the above tests themselves, then approve
