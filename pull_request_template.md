- [ ] Test that the project still successfully dbt compile 

- [ ] Test branch on a development branch of an existing client (ideally the one that raised the bug)

- [ ] do this by changing revision of the package to the branch name in `packages.yml` file in dbt

- [ ] also re-run the `create_or_update_standard_models.py` for the client dbt project, replacing models and dependencies

- [ ] check in dbt that client package successfully runs `dbt deps`, `dbt compile`, and `dbt run`; screenshot this page

- [ ] query the resulting staging model in bigquery and check that the bug is resolved; screenshot this

- [ ] Add testing requirements language and screenshots to PR

- [ ] Link bug issue to PR

- [ ] Ask for PR review from teammates: Reviewers should review code, and run the above tests themselves, then approve
