Wonder what these macros are and why they exist? Look [here for an intro](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Macros)!

For a list of all available dashboard screens/pages, click [here.](https://github.com/bsd/dbt-arc-functions/search?q=mart)

Any mart can act as the backend data model for a dashboard page.

## About the folders in the macros folder

Each folder contains a set of macros written in SQL and/or Jinja that generate compiled SQL code. This SQL code is then executed by dbt to generate the final data model.

### Suggested Implementation

Use `create_or_update_standard_models.py` as a CLI tool to create new models in the dbt github repository of the client you are working on. The program will list out the names of the folders that are contained in the macros directory of this repository, and it will ask you for the name of the models you would like to create by listing the folder names. You will have to know what set of macros apply to the client. 

For example, if you are working on a client that is reporting on email data, you will need to know that the macros for email data are contained in the `email` folder. If you know that they pull their email data from frakture warehouse's Everyaction, then you will also pull in `frakture_everyaction_email`. **Note: in most cases, we are pulling data from the global message summary in frakture, which is agnostic of CRM, so we would pull in `frakture_globalmessage_email` instead of the everyaction folder. The everyaction folder still exists because there are some nifty features if you choose to pull in CRM specific data, but that is the exception** You will then need to run `create_or_update_standard_models.py` and enter `email,frakture_everyaction_email` when prompted for the name of the models you would like to create.



`create_or_update_standard_models.py` will the output a models folder with a mart and a staging subfolder. These folders have SQL files that reference the macros you pulled in from dbt-arc-functions. It will also output a line the files that links to the code for the macros in dbt-arc-functions.

### Understanding the macros folders

Each of the macros folders contain either a `staging` and `mart` folder, or just a `staging` folder. The `staging` folders contains macros that generate SQL code that pulls data from the source and transforms it into a staging table. The `mart` folders contains macros that generate SQL code that pulls data from the staging tables and transforms it into a final data model.

#### Source-agnostic macros

For each type of data we are reporting on, regardless of where the data came from, we have a folder that is named after that data type. For example, if we are reporting on email data, we have a folder called `email`. These folders are named in plain language after the type of data that is being modeled. Folders like these contain the code for the macros that generate the final data model for the data type. In the `staging` subfolders, these folders contain the code that unions together the data from the different sources and transforms it into staging tables for different metrics and dimensions.

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

For each source we are reporting on, we have a folder that is named after that source. For example, if we are reporting on email data that was processed by Frakture from Salesforce Marketing Cloud, and it deals with person data, we have a folder called `frakture_sfmc_person`. These folders are named in plain language after the data provider, CRM, and data type. Folders like these contain the code for the macros that generate the final data model for the data type. In the `staging` subfolders, these folders contain the code that pulls data from the source and transforms it into staging tables for different metrics and dimensions. 

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