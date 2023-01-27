Wonder what these macros are and why they exist? Look [here for an intro](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Macros)!

For a list of all available dashboard screens/pages, click [here.](https://github.com/bsd/dbt-arc-functions/search?q=mart)

Any mart can act as the backend data model for a dashboard page.

## About the folders in the macros folder

- Each folder contains a set of macros written in SQL and/or Jinja that generate compiled SQL code. This SQL code is then executed by dbt to generate the final data model.
