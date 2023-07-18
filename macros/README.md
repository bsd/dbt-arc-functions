## Macros

Wonder what these macros are and why they exist? Look [here for an intro](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Macros)!

For a list of all available dashboard screens/pages, click [here.](https://github.com/bsd/dbt-arc-functions/search?q=mart)

Any mart can act as the backend data model for a dashboard page.

## About the folders in the macros folder

The macros folder in the dbt-arc-functions repository contains a set of SQL and Jinja macros that are used to generate compiled SQL code. This SQL code is then executed by dbt to generate the final data model.

The folder is organized into subfolders, each of which contains a set of macros specific to a certain data type or source.

The subfolders are divided into two types:

1. **Source-agnostic macros:** These are folders that contain macros that can be applied to any data source, and are named in plain language after the type of data being modeled. For example, if we are reporting on **email** data, we have a folder called `email`. These folders typically contain the code for the macros that generate the final data model for the data type, and in the staging subfolders, these folders contain the code that unions together the data from the different sources and transforms it into staging tables for different metrics and dimensions.

2. **Source-specific macros:** These are folders that contain macros that are specific to a certain data source, and are named in plain language after the data provider, CRM, and data type. For example, if we are reporting on email data that was processed by Frakture from Salesforce Marketing Cloud, and it deals with person data, we have a folder called `frakture_sfmc_person`. These folders typically contain the code for the macros that generate the final data model for the data type, and in the staging subfolders, these folders contain the code that pulls data from the source and transforms it into staging tables for different metrics and dimensions.

3. **Customizable macros:** In some cases, we predict that most clients will need to modify a macro. This might be when clients define custom audiencesd, need a custom naming convention, etc. In this case we create a macro with customizable in the name, which serves as a flag in the `create_or_update_standard_models.py` script to not try and rewrite that macro every time you update.

To implement the macros as models in an ARC repository, you can use the `create_or_update_standard_models.py` script as a CLI tool to create new models, or update existing models, in the dbt Github repository of the client you are working on. The program will list out the names of the folders that are contained in the macros directory of this repository, and it will ask you for the name of the models you would like to create by listing the folder names. **You will choose the source-agnostic folders based on the type of data you are looking to model: like email, email_deliverability, paidmedia. You will also have to choose the source-specific folders depending on where the client obtains its data. In most cases with frakture warehouse data, we are pulling in the global_message version of the data (ex: `frakture_global_message_email` for email).**

So you will have to know which set of macros applies to the client, and then use the script to generate the necessary SQL files and folders, which will reference the macros you pulled in from dbt-arc-functions, and also output a line the files that links to the code for the macros in dbt-arc-functions.

### Source-agnostic macros

Examples:

- email
- email_deliverability
- email_listhealth
- paidmedia
- paidmedia_goals
- paidmedia_adhoc_pacing_actuals
- paidmedia_pacing
- transactions
- combined_email_paidmedia

#### Source-specific macros

Examples:

- adhoc_google_spreadsheets_paidmedia
- frakture_actionkit_email
- frakture_everyaction_email
- frakture_everyaction_person
- frakture_global_message_email
- frakture_global_message_paidmedia
- supermetrics_yahoo_dsp_paidmedia
- frakture_global_transactions
- frakture_sfmc_person

#### Customizable macros

Examples:

- stg_stitch_sfmc_customizable_audience_transaction_jobs_append
