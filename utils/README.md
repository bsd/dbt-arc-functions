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
    - [add_dependencies.py](#add_dependenciespy)
      - [Usage](#usage-1)
      - [Plain English what it does](#plain-english-what-it-does)
    - [clone_all_tables_and_views_in_schema.ipynb](#clone_all_tables_and_views_in_schemaipynb)
      - [Usage](#usage-2)
      - [Plain English what it does](#plain-english-what-it-does-1)
    - [compile_sources.py](#compile_sourcespy)
      - [Usage](#usage-3)
      - [Plain English what it does](#plain-english-what-it-does-2)
    - [create_macros_from_sql.py](#create_macros_from_sqlpy)
      - [Usage](#usage-4)
      - [Plain English what it does](#plain-english-what-it-does-3)
    - [create_or_update_standard_models.py](#create_or_update_standard_modelspy)
      - [Usage](#usage-5)
      - [Plain English what it does](#plain-english-what-it-does-4)
    - [create_views_from_stitch_datasets.ipynb](#create_views_from_stitch_datasetsipynb)
      - [Usage](#usage-6)
      - [Plain English what it does](#plain-english-what-it-does-5)
    - [delete_schemas.py](#delete_schemaspy)
      - [Usage](#usage-7)
      - [Plain English what it does](#plain-english-what-it-does-6)
    - [first_run_dbt_project_and_profiles_fixer.py](#first_run_dbt_project_and_profiles_fixerpy)
      - [Usage](#usage-8)
      - [Plain English what it does](#plain-english-what-it-does-7)

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

## Scripts explanations

### add_dependencies.py

#### Usage
`python add_dependencies.py`

#### Plain English what it does
Our table structures are build dynamically using macros _after_ dbt does its automatic generation of the [Directed Acyclic Graph (DAG.)](https://docs.getdbt.com/docs/introduction#:~:text=dbt%20builds%20a%20directed%20acyclic,predecessor%20of%20the%20current%20model.) This means that we have to inject little `-- depends_on:` code blocks at the top of our models to let them know what previous tables they depend on. This is a pain in the butt to do manually, so this program runs dbt over and over again until it stops suggesting `-- depends_on:` blocks to add to the top of your model.

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
This function allows you to build out models based on the sources we've used before ([found here.](https://github.com/bsd/dbt-arc-functions/tree/main/macros)) This function will build a set of models in the desired project.

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
The first time you boot up a dbt project, there are a lot of variables you have to set by hand in dbt_project.yml and profiles.yml; this script does all that set up for you. You'll need to have a valid BigQuery credentials.json file to be able to run the dbt project locally, so good idea to get that together before running this script.

If you'd like to know how to generate a credentials json go [here](https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials). If you'd like to understand why you need credentials to use the CLI go [here](https://docs.getdbt.com/tutorial/create-a-project-dbt-cli
).
