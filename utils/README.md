
# utils

## Description

`utils` contains a set of functions that will allow you to set up a dbt project quickly while adhering to Blue State's standard data models, project set-up, and more!

The tools are meant to be run on a dbt project repo right after running [dbt init](https://docs.getdbt.com/reference/commands/init), which is this same as hitting `initialize your project` on a blank repo in dbt Cloud:

<img width="350" alt="Screen Shot 2021-12-14 at 12 24 56 PM" src="https://user-images.githubusercontent.com/16624855/146048553-2d141404-2b0e-4c88-b007-6733862f8048.png">
 
The utils are mean to be used in a certain order, which is outlined below. Below that, I will walk through what each function does.

That said, you can use the utils after initial set up at any time if you need to, say, add sources to a project, update your standard models, etc. It's on the todo list to create a driver function that runs all of the functions in the correct order.

## Order of Usage

1. first_run_dbt_project_and_profiles_fixer.py
2. create_or_update_standard_models.py
3. compile_sources.py
4. add_dependencies.py

## Function explanations

### Before you start

You'll need to install the required packages to run these scripts. This can be accomplished by running:
`pip install -r requirements.txt`

That said, it's highly recommended that you do this in a virtual environment managed with a tool like [Anaconda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) or [pipenv](https://pipenv.pypa.io/en/latest/). If you don't know what any of the above means... you'll probably be ok just installing these packages with pip, but it might mess up other dependencies so... you've been warned.

### add_dependencies.py

#### Usage:
`python add_dependencies.py`

#### Plain English what it does: 
Our table structures are build dynamically using macros _after_ dbt does its automatic generation of the [Directed Acyclic Graph (DAG.)](https://docs.getdbt.com/docs/introduction#:~:text=dbt%20builds%20a%20directed%20acyclic,predecessor%20of%20the%20current%20model.) This means that we have to inject little `-- depends_on:` code blocks at the top of our models to let them know what previous tables they depend on. This is a pain in the butt to do manually, so this program runs dbt over and over again until it stops suggesting `-- depends_on:` blocks to add to the top of your model.

### compile_sources.py

#### Usage:


#### Plain English what it does:


### create_macros_from_sql.py

#### Usage:


#### Plain English what it does:


### create_or_update_standard_models.py

#### Usage:


#### Plain English what it does:


### first_run_dbt_project_and_profiles_fixer.py

#### Usage:


#### Plain English what it does:

