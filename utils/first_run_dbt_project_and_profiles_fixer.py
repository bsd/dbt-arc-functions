"""This creates a project YAML and dbt_profiles YAML 
and cleans up the dbt project so that it's ready"""

#!/usr/bin/env python
# coding: utf-8
# TODO: give option of removing comments from the profiles.yml file


from os import path
from shutil import rmtree

import git
import requests
import ruamel.yaml

CREDENTIALS_HELPTEXT = """
If you'd like to know how to generate a credentials json go here: 
https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials

If you'd like to understand why you need credentials to use the CLI go here:
https://docs.getdbt.com/tutorial/create-a-project-dbt-cli

Please enter the absolute location of your credentials json file:
"""

RUN_LOCALLY_HELPTEXT = """
Would you like to be able to run this dbt project locally? [y/n]
"""

INPLACE_OR_COPY_HELPTEXT = """
Would you like to (r)eplace the existing {filename}.yml or make a (c)opy named {filename}_copy.yml?
Enter r for replace or c for copy:
"""

COPY_OR_KEEP_CREDENTIALS_HELPTEXT= """
Would you like to create a reference to your credentials file here {file_location}
 or create a copy in {profile_location}
 Enter r to reference where it is or c to copy to profiles (we recommend c):
"""

PROFILE_CHOICE_HELPTEXT = """If you'd like to use another path than {profile}, 
enter it here. Else, press return.(We recommend to press return.)
"""

packages_dict_template = {
    "packages": [
        {
            "package": "dbt-labs/dbt_utils",
            "version": "1.0.0"
        },
        {
            "git": "https://github.com/bsd/dbt-arc-functions.git",
            "revision": "v1.0.0"
        }
    ]
}

REVISION_CHOICE_HELPTEXT = """Your current active branch of dbt-arc-functions is {active_branch_name}.
For packages.yml file, if you'd like to reference another branch, or an updated revision of the repository, (for example, v4.5.0), enter it here.
Else, press return. (If you don't know what to do, press return.)
"""

DBT_ARTIFACTS_CHOICE_HELPTEXT = """Would you like to add dbt-artifacts to this repo?
dbt-artifacts produces useful artifacts in your BigQuery instance which allow you to track recent dbt runs.
Enter y for (y)es and n for (n)o. (We recommend y):
"""


def update_dbt_project(dbt_project, project_name, project_name_underscore, yaml, dbt_artifacts_choice):
    """
    Updates the `dbt_project.yml` file with the project name,
        removes the `my_new_project` model, adds a `database` variable 
        and adds the standard model directory structure.

    :param dbt_project: Path to the `dbt_project.yml` file
    :param project_name: Name of the project
    :param project_name_underscore: Name of the project with underscores
    :param yaml: YAML object
    """
    with open(dbt_project, 'r', encoding='utc-8') as f:
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
        dbt_project_yml['vars']['database'] = project_name
    else:
        dbt_project_yml['vars'] = variables
    standard_models = {'staging': {
        'materialized': 'view',
        'schema': 'staging'},
        'marts': {
            'materialized': 'table',
            'schema': 'marts'}}
    dbt_project_yml['models'][project_name_underscore] = standard_models
    if dbt_artifacts_choice == 'y':
        dbt_project_yml['on-run-end'] = ["{% if target.name == 'default' %}{{ dbt_artifacts.upload_results(results) }}{% endif %}"]
    copy_choice = inplace_or_copy("dbt_project")
    file, extension = path.splitext(dbt_project)
    with open(file + copy_choice + extension, 'w', encoding='utc-8') as f:
        yaml.dump(dbt_project_yml, f)


def copy_or_keep_credentials(credentials_location):
    """
    Copies or keeps the given credentials file at the specified location.

    :param credentials_location: The location of the credentials file.
    :return: The location of the copied or kept credentials file.
    """
    choice = ''
    _, file = path.split(credentials_location)
    profile_location = path.join(path.expanduser('~'), '.dbt', file)
    while choice not in ('r', 'c'):
        choice = input(
            COPY_OR_KEEP_CREDENTIALS_HELPTEXT.format(file_location=credentials_location,
                                                     profile_location=profile_location))
    if choice == 'c':
        with open(credentials_location, 'r', encoding='utc-8') as f:
            content = f.read()
        with open(profile_location, 'w', encoding='utc-8') as f:
            f.write(content)
        return profile_location
    return credentials_location


def update_profile_yml(project_id, project_id_underscore, yaml):
    """
    Updates the `profiles.yml` file for the project.

    :param project_id: Name of the bigquery project
    :param project_id_underscore: Name of the project with underscores
    :yaml: YAML object
    
    :return: None
    """
    choice = '' if __name__ == '__main__' else 'y'
    while choice not in ('y', 'n'):
        choice = input(RUN_LOCALLY_HELPTEXT)
    if choice == 'n':
        return '', ''
    credentials_location = input(CREDENTIALS_HELPTEXT)
    username = input("What's your company email address? Will assume username to be that.\n")
    dbt_username = f"dbt_{username.split('@')[0]}"

    credentials_location = copy_or_keep_credentials(credentials_location)
    profile_entry = {'target': 'dev',
                     'outputs': {'dev': {'type': 'bigquery',
                                         'method': 'service-account',
                                         'keyfile': credentials_location,
                                         'project': project_id,
                                         'dataset': dbt_username,
                                         'threads': 10,
                                         'timeout_seconds': 300,
                                         'location': 'US',
                                         'priority': 'interactive'}}}

    profile = path.join(path.expanduser('~'), '.dbt', 'profiles.yml')
    profile_choice = input(PROFILE_CHOICE_HELPTEXT.format(profile=profile))
    profile = profile_choice or profile
    with open(profile, 'r', encoding='utc-8') as f:
        content = f.read()
        profile_yml = yaml.load(content)

    profile_yml[project_id_underscore] = profile_entry
    copy_choice = inplace_or_copy("profile")
    file, extension = path.splitext(profile)
    with open(file + copy_choice + extension, 'w', encoding='utc-8') as f:
        yaml.dump(profile_yml, f)
    return credentials_location, dbt_username


def inplace_or_copy(filetype):
    """
    Prompts the user to choose between replacing the existing file or making a copy of the file.

    :param filetype: Type of file to be replaced or copied
    :return: '_copy' if the user wants to make a copy, else an empty string
    """
    choice = ''
    while choice not in ('r', 'c'):
        choice = input(INPLACE_OR_COPY_HELPTEXT.format(filename=filetype))
    return '_copy' if choice == 'c' else ''


def get_dbt_artifacts_with_version():
    """
    Get the latest version of dbt-artifacts from Github releases API.

    Returns:
    dict: Dictionary containing package name and version in the format:
    {'package': 'brooklyn-data/dbt_artifacts', 'version': 'x.x.x'}
    """
    github_response = requests.get(url='https://api.github.com/repos/brooklyn-data/dbt_artifacts/releases', timeout=60)
    version: str = github_response.json()[0]['tag_name']
    return {'package': 'brooklyn-data/dbt_artifacts', 'version': version}

def write_packages_yml(dbt_packages_path, active_branch_name, yaml, dbt_artifacts_choice):
    """
    Write a 'packages.yml' file to the given `dbt_packages_path` with the current revision set to the given
    `active_branch_name`. The `yaml` object is used to dump the `packages_dict` to the file. If a 'packages.yml'
    file already exists at the given path, the user is prompted to confirm whether they want to replace the file.
    """
    packages_dict = packages_dict_template.copy()
    revision_choice = input(REVISION_CHOICE_HELPTEXT.format(
        active_branch_name=active_branch_name))
    revision = revision_choice or active_branch_name
    packages_dict['packages'][1]['revision'] = revision
    if dbt_artifacts_choice == 'y':
        packages_dict['packages'].append(get_dbt_artifacts_with_version())
    if not path.exists(dbt_packages_path):
        with open(dbt_packages_path, 'w', encoding='utc-8') as f:
            yaml.dump(packages_dict, f)
    elif input(f"Would you like to replace packages.yml with:\n{packages_dict}\n(y/n)\n") == 'y':
        with open(dbt_packages_path, 'w', encoding='utc-8') as f:
            yaml.dump(packages_dict, f)


def get_active_branch_name():
    """Get the name of the current active branch for the current repository.
    If the current commit is a tag,
    returns the name of the tag.
    
    Returns:
        str: The name of the current active branch or tag.
    """
    repo = git.Repo(search_parent_directories=True)
    current_commit = repo.commit()
    return next(
        (str(tag) for tag in repo.tags if current_commit == tag.commit),
        repo.active_branch.name,
    )


def main():
    """
    main

    This function modifies an existing dbt_project.yml file by changing the name, 
        adding a package and a model, updating the
        credentials file reference in the profile.yml file, 
        and deleting the 'models/example' folder.

    :param: None
    :return: tuple of str - a string path to the modified dbt_project.yml file, 
            the project id, a YAML object, the path to the credentials file, 
            and the dbt username
    """
    dbt_project_path = input("Please enter the full path of the dbt_project.yml you'd like to modify:\n")
    path.dirname(dbt_project_path)
    project_id = input("\nPlease enter the name of the Google Project (should look like bsd-projectname):\n")
    project_id_underscore = project_id.replace('-', '_')
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    dbt_artifacts_choice = None
    while dbt_artifacts_choice not in ('y', 'n'):
        dbt_artifacts_choice = input(dbt_artifacts_choice_helptext)
    update_dbt_project(dbt_project_path, project_id, project_id_underscore, yaml, dbt_artifacts_choice)
    dbt_base_path = path.dirname(dbt_project_path)
    dbt_packages_path = path.join(dbt_base_path, 'packages.yml')
    active_branch_name = get_active_branch_name()
    write_packages_yml(dbt_packages_path, active_branch_name, yaml, dbt_artifacts_choice)
    dbt_example_path = path.join(dbt_base_path, 'models', 'example')
    if (
        path.exists(dbt_example_path)
        and input("\nCan I delete 'models/example'? (y/n)\n") == 'y'
    ):
        rmtree(dbt_example_path)
    dbt_credentials_path, dbt_username = update_profile_yml(project_id, project_id_underscore, yaml)
    print("Program terminated successfully!")
    return dbt_project_path, project_id, yaml, dbt_credentials_path, dbt_username


if __name__ == '__main__':
    main()
