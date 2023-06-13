# Pull Request for dbt-arc-functions

- [ ] Open the pull request as a draft so that you can run tests against the code and inspect the files in the Files view of the Pull Request interface in Github.

- [ ] Once you're satisfied, convert the draft into an open Pull Request by clicking "Ready for Review" near the bottom of this page. You should not make any changes to the code except in dire emergency once you've converted from draft, so make sure you're really satisfied.

- [ ] Add two of the principal contributors to this PR for review. Currently, the principal contributors are @dmbluestate , @Frydafly , and @ryantimjohn.

- [ ] We've divided our PR templates into Macros, Utils and Docs PR requests. Delete the ones you're not using.
### Macros (delete if not using)

- [ ] Link Jira tickets (or Github Issues) to PR here. Make sure that the Jira ticket has a good description as to why this PR is necessary and the business problem that this PR is solving. Moreover, if there have been any discussions of this ticket in Slack, make sure they're linked in the Jira ticket:

- [ ] Make sure this is named using Jira extension so that naming convention is consistent. If it's not, that's okay. Go to the Jira ticket, click into the ticket, then on the right under Details, find Development, then click the Create branch button. Copy the Branch Name from there. Read here [how to rename a branch in Github](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/renaming-a-branch) to rename this branch. In the future, create branches from Jira.

- [ ] Make sure the Macro is documented by running the notebook: `create_docs.ipynb` util in this repo. From there, you'll have to either fill in descriptions by hand OR use our `fill_in_descriptions_using_openai.ipynb` to fill in descriptions using OpenAI's ChatGPT. Always make sure to check over ChatGPT's column descriptions, as they're likely not perfect.

- [ ] Test branch on a development branch of an existing client (ideally the one that raised the bug). Do this by changing revision of the package to the branch name in `packages.yml` file in dbt State client below.

- [ ] If new macro files were created or updated, re-run `create_or_update_standard_models.py` for the client dbt project, replacing models and dependencies

- [ ] check in dbt that client package successfully runs `dbt deps`, `dbt compile`, and `dbt run`; screenshot this page

- [ ] query the resulting staging model in bigquery (or by previewing mart in dbt cloud, or your IDE); screenshot this

### Utils (delete if not using)

- [ ] Link Jira tickets (or Github Issues) to PR here. Make sure that the Jira ticket has a good description as to why this PR is necessary and the business problem that this PR is solving. Moreover, if there have been any discussions of this ticket in Slack, make sure they're linked in the Jira ticket:

- [ ] Make sure this is named using Jira extension so that naming convention is consistent. If it's not, that's okay. Go to the Jira ticket, click into the ticket, then on the right under Details, find Development, then click the Create branch button. Copy the Branch Name from there. Read here [how to rename a branch in Github](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/renaming-a-branch) to rename this branch. In the future, create branches from Jira.

- [ ] Suggest any tests that your PR reviewer might use to check the utils here. Better yet, if you're able, write automated tests in Github Actions!

### Docs (delete if not using)

- [ ] Link Jira tickets (or Github Issues) to PR here. Make sure that the Jira ticket has a good description as to why this PR is necessary and the business problem that this PR is solving. Moreover, if there have been any discussions of this ticket in Slack, make sure they're linked in the Jira ticket. If this is a very simple docs change, feel free to write a brief outline below instead:

## Formatters

Three different formatters will run when you open a pull request:
- [sqlfmt](http://sqlfmt.com/)
- [yamlfix](https://lyz-code.github.io/yamlfix/)
- [Black(python)](https://pypi.org/project/black/)

If any of them detect syntax problems, they will error. They will automatically open a pull request with the changes you need to make to pass the formatter. Just merge that PR and you should be good to go.

## Linters
We have two in-house linters:

### [check_for_docs_for_every_macro.py](https://github.com/bsd/dbt-arc-functions/blob/main/utils/check_for_docs_for_every_macro.py)
Just makes sure that every macro has a doc associated with it in the documentation folder with the same folder structure, i.e.:

`documentation/combined_email_paidmedia/marts/mart_combined_email_paidmedia_daily_revenue_performance.yml`

is the doc for

`       macros/combined_email_paidmedia/marts/mart_combined_email_paidmedia_daily_revenue_performance.sql`

### [check_docs_correct.py](https://github.com/bsd/dbt-arc-functions/blob/main/utils/check_docs_correct.py)
Runs through a lot of checks on documents to make sure that they are formatted to our standards. This includes checking whether docs and sources:
- have columns
- have tables
- source columns have data_type and description
- have a version number
- docs have a macro associated with them
- docs are not blank

There may be additional checks not documented here. The linter will fail on any error.

The script will document all failures and display them to you with remedies.
