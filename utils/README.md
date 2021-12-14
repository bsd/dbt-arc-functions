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

### add_dependencies.py

### compile_sources.py
