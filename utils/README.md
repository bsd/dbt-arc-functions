# utils

Look [here](https://bluestate.atlassian.net/wiki/spaces/ATeam/pages/2986049548/Technical+introduction+to+ARC#Utils) for an intro to why these utils exist!

## Table of Contents
- [utils](#utils)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [main.py](#mainpy)
    - [Before you start](#before-you-start)
    - [Usage](#usage)
    - [About](#about)
    - [Order of scripts used by main.py](#order-of-scripts-used-by-mainpy)
  - [Scripts explanations](#scripts-explanations)
    - [add\_dependencies.py](#add_dependenciespy)
      - [Usage](#usage-1)
      - [Plain English what it does](#plain-english-what-it-does)
    - [add\_github\_actions\_workflows.py](#add_github_actions_workflowspy)
      - [Usage](#usage-2)
      - [Plain English what it does](#plain-english-what-it-does-1)
    - [clone\_all\_tables\_and\_views\_in\_schema.ipynb](#clone_all_tables_and_views_in_schemaipynb)
      - [Usage](#usage-3)
      - [Plain English what it does](#plain-english-what-it-does-2)
    - [compile\_sources.py](#compile_sourcespy)
      - [Usage](#usage-4)
      - [Plain English what it does](#plain-english-what-it-does-3)
    - [create\_macros\_from\_sql.py](#create_macros_from_sqlpy)
      - [Usage](#usage-5)
      - [Plain English what it does](#plain-english-what-it-does-4)
    - [create\_or\_update\_standard\_models.py](#create_or_update_standard_modelspy)
      - [Usage](#usage-6)
      - [Plain English what it does](#plain-english-what-it-does-5)
        - [UPDATE 2023/06/23](#update-20230623)
        - [UPDATE 2023/07/13](#update-20230713)
        - [UPDATE 2023/07/20](#update-20230720)
    - [create\_views\_from\_stitch\_datasets.ipynb](#create_views_from_stitch_datasetsipynb)
      - [Usage](#usage-7)
      - [Plain English what it does](#plain-english-what-it-does-6)
    - [delete\_schemas.py](#delete_schemaspy)
      - [Usage](#usage-8)
      - [Plain English what it does](#plain-english-what-it-does-7)
    - [first\_run\_dbt\_project\_and\_profiles\_fixer.py](#first_run_dbt_project_and_profiles_fixerpy)
      - [Usage](#usage-9)
      - [Plain English what it does](#plain-english-what-it-does-8)
    - [process\_model\_documentation\_codegen\_output.ipynb](#process_model_documentation_codegen_outputipynb)
      - [Usage](#usage-10)
      - [Plain English what it does](#plain-english-what-it-does-9)

## Description

`utils` contains a set of functions that will allow you to set up a dbt project quickly while adhering to Blue State's standard data models, project set-up, and more!

The tools are meant to be run on a dbt project repo right after running [dbt init](https://docs.getdbt.com/reference/commands/init), which is this same as hitting `initialize your project` on a blank repo in dbt Cloud:

<img width="350" alt="Screen Shot 2021-12-14 at 12 24 56 PM" src="https://user-images.githubusercontent.com/16624855/146048553-2d141404-2b0e-4c88-b007-6733862f8048.png">
 
The utils are mean to be used in a certain order, which is outlined below. Below that, I will walk through what each function does.

That said, you can use the utils after initial set up at any time if you need to, say, add sources to a project, update your standard models, etc.

## main.py

### Before you start

You'll need to install the required packages to run these scripts. This can be accomplished by running:

I recommend [pipenv](https://pipenv.pypa.io/en/latest/) for this as ruamel doesn't play nicely with Anaconda. 

To install, run:
`pip install pipenv`

Then, run:
```
pipenv install
pipenv shell
```

This will get you into an environment ready made to run these files.

### Usage
`python main.py`

### About
`main.py` Is the first function you should run when spinning up a new dbt project. It will run an number of scripts that will update .yml files, create .sql models, and generally get your project working in short order.

### Order of scripts used by main.py

1. [first_run_dbt_project_and_profiles_fixer.py](#first_run_dbt_project_and_profiles_fixerpy)
2. [create_or_update_standard_models.py](#create_or_update_standard_modelspy)
3. [compile_sources.py](#compile_sourcespy)
4. [delete_schemas.py](#delete_schemaspy)
5. [add_dependencies.py](#add_dependenciespy)
6. [add_github_actions_workflows.py](#add_github_actions_workflowspy)


## Scripts explanations

### add_dependencies.py

#### Usage
`python add_dependencies.py` (this will run you through the script step by step)

`python add_dependencies.py --dbt_base_path "/path/to/dbt/project"` (this will run the script with the path you provide in one step)

#### Plain English what it does
Our table structures are build dynamically using macros _after_ dbt does its automatic generation of the [Directed Acyclic Graph (DAG.)](https://docs.getdbt.com/docs/introduction#:~:text=dbt%20builds%20a%20directed%20acyclic,predecessor%20of%20the%20current%20model.) This means that we have to inject little `-- depends_on:` code blocks at the top of our models to let them know what previous tables they depend on. This is a pain in the butt to do manually, so this program runs dbt over and over again until it stops suggesting `-- depends_on:` blocks to add to the top of your model.

---

### add_github_actions_workflows.py

#### Usage
`python add_github_actions_workflows.py`

#### Plain English what it does
We'd like to automate as much of our build process as possible. This script allows us to add Github Actions Workflows to all new projects. Github Actions is a CI/CD tool which is integrated into Github.

This script is a utility to automate the setup of a dbt project in Github and dbt Cloud. It prompts the user to ensure they have a working dbt repository in Github, a dbt Cloud project set up, and their dbt Cloud API key. It then prompts the user to add their API key as a secret to their Github repository, and to provide the URL of their dbt Cloud job. The script then creates a workflow in Github Actions, with the trigger set to either a merge to the main branch or a pull request, depending on whether the environment is set to 'prod' or 'dev'.

---

### clone_all_tables_and_views_in_schema.ipynb

#### Usage
Suggest you run this as a notebook in VertexAI Workbench as it will handle credentials for you.

#### Plain English what it does
NOTE: Table clones are quite slow when tables are partitioned. Each table clone takes about the same amount of time, but with a partitioned table the script functionally has to make thousands of clones.

Sometimes it's helpful to clone all the tables in one schema (say a `prod_rep` schema) to another (say a `dbt_your_username_rep` schema) for testing purposes, especially when you're upgrading versions of dbt and want to do a `dbt run` but don't want it to make new versions of all your incremental models. Clones in BQ are very nice because they're basically free.

Read more here:
[Introduction to table clones](https://cloud.google.com/bigquery/docs/table-clones-intro)

---


### compile_sources.py

#### Usage
`python compile_sources.py`

#### Plain English what it does
Generally, we try and roll up all of our source tables from a single data provider into a single view so we can select from that as a source. Sometimes, that isn't possible. In those cases, we need to build out a list of tables to select from. This function builds out that list of tables for you.

---

### create_macros_from_sql.py

#### Usage
`python create_macros_from_sql.py`

#### Plain English what it does
When you're prototyping building out a set of models, it's easiest to do that in plain SQL. Going from plain SQL to functional macros that you can reuse in different projects is a pain. This script does that for you.

---

### create_or_update_standard_models.py

#### Usage
`python create_or_update_standard_models.py`

#### Plain English what it does
This function allows you to build out models based on the sources we've used before ([found here.](https://github.com/bsd/dbt-arc-functions/tree/main/macros)) This function will build a set of models in the desired project, by generating sql files that reference macros within a folder structure recognized by dbt.
##### UPDATE 2023/06/23
Now works with snapshots
##### UPDATE 2023/07/13
Now works with \_customizable\_ sql.

##### UPDATE 2023/07/20
Now works with \_parameterized\_ sql.

---

### create_views_from_stitch_datasets.ipynb

#### Usage
We suggest you run this as a notebook in VertexAI Workbench as it will handle credentials for you.

#### Plain English what it does
Our Stitch data all goes to a single Google Cloud Project and therefore BigQuery database. You're going to want to access that data from the client-specific GCP Projects and BigQuery databases. This script finds all datasets in the `bsd-stitch` BigQuery database that match the client shortname (i.e. `oxfam` for Oxfam, `msf` for Doctors Without Borders) and offers to copy each one of them over to the client-BigQuery instance as a dataset full of views. The dataset name willl be `src_stitch_{the_name_of_dataset_in_stitch_bq_instance}`

---

### delete_schemas.py

#### Usage
`python delete_schemas.py`

#### Plain English what it does
This function deletes the specific schemas that your dbt profile has created so that you can start from a clean slate. This is particularly useful in our configuration because we use regex to roll up names of tables, and if you have old, unused tables in your schema this can cause unforseen errors.

---

### first_run_dbt_project_and_profiles_fixer.py

#### Usage
`python first_run_dbt_project_and_profiles_fixer.py`

#### Plain English what it does
The first time you boot up a dbt project, there are a lot of variables you have to set by hand in dbt_project.yml and profiles.yml; this script does all that set up for you. Includes adding +persist_docs to all new `dbt_project.yml` . You'll need to have a valid BigQuery credentials.json file to be able to run the dbt project locally, so good idea to get that together before running this script.

If you'd like to know how to generate a credentials json go [here](https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials). If you'd like to understand why you need credentials to use the CLI go [here](https://docs.getdbt.com/tutorial/create-a-project-dbt-cli
).

---

### process_model_documentation_codegen_output.ipynb

#### Usage
We suggest using this locally after you've run codegen locally.

#### Plain English what it does
This function allows you to process the output of the dbt-codegen package to add documentation skeletons to documentation in the `dbt-arc-functions/documentation` folder.

Firstly, you'll have to have a working repo using the macro that you want to document. The model has to compile and run for this function to work. Additionally you'll have to have the codegen package installed in the working repo:
https://hub.getdbt.com/dbt-labs/codegen/latest/

Then, either in the web IDE or locally, run generate model yaml. Instructions can be found here:
https://github.com/dbt-labs/dbt-codegen#generate_model_yaml-source

Save the output from the above function to a file and insert that file's name into the `codegen_output` variable in cell 2 of `process_model_documentation_codegen_output.ipynb`. 

Finally, run all cells in the notebook. Examine the changes in the files and, if you're satisfied, commit and push changes to Github.
