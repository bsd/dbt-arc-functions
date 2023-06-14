[![Black](https://github.com/bsd/dbt-arc-functions/actions/workflows/python_black.yml/badge.svg)](https://github.com/bsd/dbt-arc-functions/actions/workflows/python_black.yml)

# dbt ARC Functions

Functions used across clients using dbt in Analytics Reporting Cloud.

Wonder why this repo exists? Look [here](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Data-transformation)!

## Currently available products

Look [here in macros](https://github.com/bsd/dbt-arc-functions/tree/main/macros) for products we currently support.

Look [here in utils](https://github.com/bsd/dbt-arc-functions/tree/main/utils) for utilities that will help you get a dbt project going faster!

## How to Contribute

1. Claim an issue from the Jira tickets or Github Issues.
2. Create new branch from the Jira ticket. To do this, go to the Jira ticket, click into the ticket, then on the right under `Details`, find `Development`, then click the `Create branch` button.
3. Open the pull request as a draft so that you can run tests against the code and inspect the files in the Files view of the Pull Request interface in Github.
4.  Link Jira tickets (or Github Issues) to PR. Make sure that the Jira ticket has a good description as to why this PR is necessary and the business problem that this PR is solving. Moreover, if there have been any discussions of this ticket in Slack, make sure they're linked in the Jira ticket.
5. Once you're satisfied, convert the draft into an open Pull Request by clicking "Ready for Review" near the bottom of the PR page. You should not make any changes to the code except in dire emergency once you've converted from draft, so make sure you're really satisfied.
6. Add two of the principal contributors to this PR for review. Currently, the principal contributors are @dmbluestate , @Frydafly , and @ryantimjohn.
7. Reviewers should review the PR within 48 hours. If their work load prevents them from getting to the PR review or they are unable to complete the review because of incomplete information, they should let you know within that timeframe.
8. If a review isn't approved, status should be changed to Request Changes with in line comments on the code and, potentially, general comments.
9. Once the PR is approved, the requester should merge the PR.
10. Make sure to delete your branch after you're done.

## How to Submit Bugs

* usually, bug is identified by blue state client or analyst on account

* bug is confirmed to be a result of the dbt-arc-functions by engineer

* bug report created in [ARC Jira project](https://bluestate.atlassian.net/jira/software/c/projects/ARC/boards/245?selectedIssue=ARC-753&quickFilter=580)

* create bug report as an issue in the GitHub project, adding a) what you expect to see, b) what you actually see, c) include screenshots and links + logging wherever possible
