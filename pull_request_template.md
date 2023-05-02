# Pull Request for dbt-arc-functions

- [ ] Create branch of dbt-arc-functions using JIRA extension so that naming convention is consistent

- [ ] If you've created a macro, make sure it is documented by running the notebook: `create_docs.ipynb` util in this repo

- [ ] Test branch on a development branch of an existing client (ideally the one that raised the bug). Do this by changing revision of the package to the branch name in `packages.yml` file in dbt State client below.

- [ ] If new macro files were created or updated, re-run `create_or_update_standard_models.py` for the client dbt project, replacing models and dependencies

- [ ] check in dbt that client package successfully runs `dbt deps`, `dbt compile`, and `dbt run`; screenshot this page

- [ ] query the resulting staging model in bigquery (or by previewing mart in dbt cloud, or your IDE); screenshot this

- [ ] If exists, link bug issue and jira tickets to PR
