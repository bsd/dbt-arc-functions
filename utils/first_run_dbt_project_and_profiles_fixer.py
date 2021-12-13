#!/usr/bin/env python
# coding: utf-8
# TODO: give option of removing comments
# params['ParentTest']['test'].yaml_add_eol_comment('some comment', key='new_key', column=40) # column is optional


from os import path

import ruamel.yaml

credentials_helptext = """
If you'd like to know how to generate a credentials json go here: 
https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials

If you'd like to understand why you need credentials to use the CLI go here:
https://docs.getdbt.com/tutorial/create-a-project-dbt-cli

Please enter the absolute location of your credentials json file:
"""

run_locally_helptext = """
Would you like to be able to run this dbt project locally? [y/n]
"""
inplace_or_copy_helptext = """
Would you like to (r)eplace the existing {filename}.yml or make a (c)opy named {filename}_copy.yml?
Enter r for replace or c for copy:
"""

copy_or_keep_credentials_helptext = """
Would you like to create a reference to your credentials file here {file_location}
 or create a copy in {profile_location}
 Enter r to reference where it is or c to copy to profiles (we recommend c):
"""

profile_choice_helptext = """If you'd like to use another path than {profile}, enter it here. Else, press return.
 (We recommend to press return.)
"""

def update_dbt_project(dbt_project, project_name, project_name_underscore, yaml):
    with open(dbt_project, 'r') as f:
        content = f.read()
        dbt_project_yml = yaml.load(content)

    dbt_project_yml['name'] = project_name_underscore
    dbt_project_yml['profile'] = project_name_underscore
    if 'my_new_project' in dbt_project_yml['models']:
        remove_new_project = input('\nCan I remove my_new_project from dbt_project? [y/n]\n')
        if remove_new_project == 'y':
            del dbt_project_yml['models']['my_new_project']
    variables = {'database': project_name}
    if 'vars' in dbt_project_yml:
        dbt_project['vars']['database'] = project_name
    else:
        dbt_project_yml['vars']= variables
    standard_models = {'staging': {
        'materialized': 'view',
        'schema': 'staging'},
        'marts': {
            'materialized': 'table',
            'schema': 'marts'}}
    dbt_project_yml['models'][project_name_underscore] = standard_models
    copy_choice = inplace_or_copy("dbt_project")
    file, extension = path.splitext(dbt_project)
    with open(file + copy_choice + extension, 'w') as f:
        yaml.dump(dbt_project_yml, f)


def copy_or_keep_credentials(credentials_location):
    choice = ''
    _, file = path.split(credentials_location)
    profile_location = path.join(path.expanduser('~'), '.dbt', file)
    while choice not in ('r', 'c'):
        choice = input(
            copy_or_keep_credentials_helptext.format(file_location=credentials_location, profile_location=profile_location))
    if choice == 'c':
        with open(credentials_location,'r') as f:
            content = f.read()
        with open(profile_location, 'w') as f:
            f.write(content)
        return profile_location
    return credentials_location


def update_profile_yml(project_name, yaml):
    choice = ''
    while choice not in ('y', 'n'):
        choice = input(run_locally_helptext)
    if choice == 'n':
        return
    credentials_location = input(credentials_helptext)
    username = input("What's your company email address? Will assume username to be that.\n")
    dbt_username = f"dbt_{username.split('@')[0]}"

    credentials_location = copy_or_keep_credentials(credentials_location)
    profile_entry = {'target': 'dev',
                     'outputs': {'dev': {'type': 'bigquery',
                                         'method': 'service-account',
                                         'keyfile': credentials_location,
                                         'project': project_name,
                                         'dataset': dbt_username,
                                         'threads': 10,
                                         'timeout_seconds': 300,
                                         'location': 'US',
                                         'priority': 'interactive'}}}

    profile = path.join(path.expanduser('~'), '.dbt', 'profiles.yml')
    profile_choice = input(profile_choice_helptext.format(profile=profile))
    profile = profile_choice if profile_choice else profile
    with open(profile, 'r') as f:
        content = f.read()
        profile_yml = yaml.load(content)

    profile_yml[project_name_underscore] = profile_entry
    copy_choice = inplace_or_copy("profile")
    file, extension = path.splitext(profile)
    with open(file + copy_choice + extension,'w') as f:
        yaml.dump(profile_yml, f)


def inplace_or_copy(filetype):
    choice = ''
    while choice not in ('r', 'c'):
        choice = input(inplace_or_copy_helptext.format(filename=filetype))
    return '_copy' if choice == 'c' else ''


if __name__ == '__main__':
    dbt_project = input("Please enter the full path of the dbt_project.yml you'd like to modify:\n")
    project_name = input("\nPlease enter the name of the Google Project (should look like bsd-projectname):\n")
    project_name_underscore = project_name.replace('-', '_')
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    update_dbt_project(dbt_project, project_name, project_name_underscore, yaml)
    update_profile_yml(project_name, yaml)
    print("Program terminated successfully!")
