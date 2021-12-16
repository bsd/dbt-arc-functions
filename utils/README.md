# utils
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
    - [compile_sources.py](#compile_sourcespy)
      - [Usage](#usage-2)
      - [Plain English what it does](#plain-english-what-it-does-1)
    - [create_macros_from_sql.py](#create_macros_from_sqlpy)
      - [Usage](#usage-3)
      - [Plain English what it does](#plain-english-what-it-does-2)
    - [create_or_update_standard_models.py](#create_or_update_standard_modelspy)
      - [Usage](#usage-4)
      - [Plain English what it does](#plain-english-what-it-does-3)
    - [first_run_dbt_project_and_profiles_fixer.py](#first_run_dbt_project_and_profiles_fixerpy)
      - [Usage](#usage-5)
      - [Plain English what it does](#plain-english-what-it-does-4)

## Description

`utils` contains a set of functions that will allow you to set up a dbt project quickly while adhering to Blue State's standard data models, project set-up, and more!

The tools are meant to be run on a dbt project repo right after running [dbt init](https://docs.getdbt.com/reference/commands/init), which is this same as hitting `initialize your project` on a blank repo in dbt Cloud:

<img width="350" alt="Screen Shot 2021-12-14 at 12 24 56 PM" src="https://user-images.githubusercontent.com/16624855/146048553-2d141404-2b0e-4c88-b007-6733862f8048.png">
 
The utils are mean to be used in a certain order, which is outlined below. Below that, I will walk through what each function does.

That said, you can use the utils after initial set up at any time if you need to, say, add sources to a project, update your standard models, etc.

## main.py

### Before you start

You'll need to install the required packages to run these scripts. This can be accomplished by running:
`pip install -r requirements.txt`

That said, it's highly recommended that you do this in a virtual environment managed with a tool like [Anaconda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) or [pipenv](https://pipenv.pypa.io/en/latest/). If you don't know what any of the above means... you'll probably be ok just installing these packages with pip, but it might mess up other dependencies so... you've been warned.

### Usage
`python main.py`

### About
`main.py` Is the first function you should run when spinning up a new dbt project. It will run an number of scripts that will update .yml files, create .sql models, and generally get your project working in short order.

### Order of scripts used by main.py

1. [first_run_dbt_project_and_profiles_fixer.py](#first_run_dbt_project_and_profiles_fixerpy)
2. [create_or_update_standard_models.py](#create_or_update_standard_modelspy)
3. [compile_sources.py](#compile_sourcespy)
4. [add_dependencies.py](#add_dependenciespy)

## Scripts explanations

### add_dependencies.py

#### Usage
`python add_dependencies.py`

#### Plain English what it does
Our table structures are build dynamically using macros _after_ dbt does its automatic generation of the [Directed Acyclic Graph (DAG.)](https://docs.getdbt.com/docs/introduction#:~:text=dbt%20builds%20a%20directed%20acyclic,predecessor%20of%20the%20current%20model.) This means that we have to inject little `-- depends_on:` code blocks at the top of our models to let them know what previous tables they depend on. This is a pain in the butt to do manually, so this program runs dbt over and over again until it stops suggesting `-- depends_on:` blocks to add to the top of your model.

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

### first_run_dbt_project_and_profiles_fixer.py

#### Usage
`python first_run_dbt_project_and_profiles_fixer.py`

#### Plain English what it does
The first time you boot up a dbt project, there are a lot of variables you have to set by hand in dbt_project.yml and profiles.yml; this script does all that set up for you. You'll need to have a valid BigQuery credentials.json file to be able to run the dbt project locally, so good idea to get that together before running this script.

If you'd like to know how to generate a credentials json go [here](https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials). If you'd like to understand why you need credentials to use the CLI go [here](https://docs.getdbt.com/tutorial/create-a-project-dbt-cli
).
