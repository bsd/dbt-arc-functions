"""This creates a project YAML and dbt_profiles YAML
and cleans up the dbt project so that it's ready"""

# coding: utf-8
# TODO: give option of removing comments from the profiles.yml file
# TODO: give option of removing comments from the dbt_project.yml file
# TODO: give option of using a more updated version of dbt in the
# TODO: ask user for project timezone and add response to the key value pair
# dbt_project.yml file


from os import path
from shutil import rmtree
from utils import initialize_yaml
from datetime import datetime

import git
import requests
import pytz

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

COPY_OR_KEEP_CREDENTIALS_HELPTEXT = """
Would you like to create a reference to your credentials file here {file_location}
 or create a copy in {profile_location}
 Enter r to reference where it is or c to copy to profiles (we recommend c):
"""

PROFILE_CHOICE_HELPTEXT = """If you'd like to use another path than {profile},
enter it here. Else, press return.(We recommend to press return.)
"""

packages_dict_template = {
    "packages": [
        {"package": "dbt-labs/dbt_utils", "version": "1.0.0"},
        {"git": "https://github.com/bsd/dbt-arc-functions.git", "revision": "v4.5.0"},
        {"package": "calogica/dbt_date", "version": "0.7.2"},
    ]
}

# TODO: ask user if they want their active branch or the latest release of
# dbt-arc-functions
REVISION_CHOICE_HELPTEXT = """Your current active branch of dbt-arc-functions is {active_branch}

The most recent release is {most_recent_release}

It's best if you use a branch that's checked out otherwise models might not be the same.
Please type the release or branch you'd like to use (pressing return will choose active branch):
"""

DBT_ARTIFACTS_CHOICE_HELPTEXT = """Would you like to add dbt-artifacts to this repo?
dbt-artifacts produces useful artifacts in your BigQuery instance which allow you to track recent dbt runs.
Enter y for (y)es and n for (n)o. (We recommend y):
"""

TIMEZONE_CHOICE_HELPTEXT = """Would you like to add a timezone to this project?
This should be the timezone of your end-users so that they see data coming in on the
 date + time they see in other products they use.
This should be in the "timezone string" format, for reference:
https://documentation.mersive.com/content/topics/api-timezones.htm

Providing the US ones from East to West below so you can copy-paste:

America/New_York
America/Chicago
America/Denver
America/Los_Angeles

If you enter nothing, will default to UTC. If you enter an invalid timezone string, will reprompt:
"""


def get_timezone_choice():
    while True:
        timezone_choice = input(TIMEZONE_CHOICE_HELPTEXT) or "UTC"
        if timezone_choice in pytz.all_timezones:
            break
    return timezone_choice


FISCAL_YEAR_CHOICE_HELPTEXT = """Would you like to add a fiscal year start date to the project?

This is for clients with a different fiscal year to the calendar year.
If you enter nothing, fiscal year will default to the calendar year.

Please enter in the format MM-DD.
If you enter an invalid date, will reprompt:
"""


def get_fiscal_year_start_choice():
    while True:
        fiscal_year_start_choice = input(FISCAL_YEAR_CHOICE_HELPTEXT) or "01-01"
        try:
            datetime.strptime(fiscal_year_start_choice, "%m-%d")
            break
        except ValueError as e:
            print(f"\n\n!!!Not a valid date in MM-DD format!!!!\n\n{e}")
    return fiscal_year_start_choice


def update_dbt_project(
    dbt_project, project_name, project_name_underscore, yaml, dbt_artifacts_choice
):
    """
    Updates the `dbt_project.yml` file with the project name,
        removes the `my_new_project` model, adds a `database` variable
        and adds the standard model directory structure.

    :param dbt_project: Path to the `dbt_project.yml` file
    :param project_name: Name of the project
    :param project_name_underscore: Name of the project with underscores
    :param yaml: YAML object
    """
    # TODO: add a check to see if the dbt_project.yml file is already updated
    # TODO: check if user supplied a valid path to the dbt_project.yml file
    with open(dbt_project, "r", encoding="utf-8") as f:
        # loads the dbt_project.yml file
        content = f.read()
        dbt_project_yml = yaml.load(content)

    dbt_project_yml["name"] = project_name_underscore
    # assigns the project name to the name key in the dbt_project.yml file
    dbt_project_yml["profile"] = project_name_underscore
    # assigns the profile name to the profile key in the dbt_project.yml file
    if "my_new_project" in dbt_project_yml["models"]:
        remove_new_project = input(
            "\nCan I remove my_new_project from dbt_project? [y/n]\n"
        )
        if remove_new_project == "y":
            del dbt_project_yml["models"]["my_new_project"]
    variables = {"database": project_name}
    if "vars" in dbt_project_yml:
        # checks if the vars key is in the dbt_project.yml file
        dbt_project_yml["vars"]["database"] = project_name
    else:
        # if the vars key is not in the dbt_project.yml file, it adds it
        dbt_project_yml["vars"] = variables
    # adds the standard model directory structure
    standard_models = {
        "staging": {"materialized": "view", "schema": "staging"},
        "marts": {"materialized": "table", "schema": "marts"},
    }
    dbt_project_yml["models"][project_name_underscore] = standard_models
    # adds dbt-artifacts to the on-run-end hook
    if dbt_artifacts_choice == "y":
        dbt_project_yml["on-run-end"] = [
            "{% if target.name == 'default' %}{{ dbt_artifacts.upload_results(results) }}{% endif %}"
        ]
    # adds dbt-date to the vars and sets default timezone conversion as UTC
    # TODO: ask user for timezone conversion for their client and add it to the vars
    # TODO: list out the available timezones, it is easy to make a typo
    timezone_choice = get_timezone_choice()
    dbt_project_yml["vars"]["dbt_date:time_zone"] = timezone_choice
    fiscal_year_start = get_fiscal_year_start_choice()
    dbt_project_yml["vars"]["fiscal_year_start"] = fiscal_year_start
    copy_choice = inplace_or_copy("dbt_project")
    file, extension = path.splitext(dbt_project)
    dbt_project_yml["models"]["+persist_docs"] = {"relation": True, "columns": True}
    with open(file + copy_choice + extension, "w", encoding="utf-8") as f:
        # width is set to a large number to avoid line breaks
        yaml.dump(dbt_project_yml, f)


def copy_or_keep_credentials(credentials_location):
    """
    Copies or keeps the given credentials file at the specified location.

    :param credentials_location: The location of the credentials file.
    :return: The location of the copied or kept credentials file.
    """
    choice = ""
    _, file = path.split(credentials_location)
    profile_location = path.join(path.expanduser("~"), ".dbt", file)
    while choice not in ("r", "c"):
        choice = input(
            COPY_OR_KEEP_CREDENTIALS_HELPTEXT.format(
                file_location=credentials_location, profile_location=profile_location
            )
        )
    if choice == "c":
        with open(credentials_location, "r", encoding="utf-8") as f:
            content = f.read()
        with open(profile_location, "w", encoding="utf-8") as f:
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
    choice = "" if __name__ == "__main__" else "y"
    # if the script is being run from the command line, ask the user if they
    # want to run locally
    while choice not in ("y", "n"):
        choice = input(RUN_LOCALLY_HELPTEXT)
    if choice == "n":
        return "", ""
    # if the user wants to run locally, ask them for the credentials file
    # location
    credentials_location = input(CREDENTIALS_HELPTEXT)
    username = input(
        "What's your company email address? Will assume username to be that.\n"
    )
    dbt_username = f"dbt_{username.split('@')[0]}"

    # copy the credentials file to the .dbt folder
    credentials_location = copy_or_keep_credentials(credentials_location)
    profile_entry = {
        "target": "dev",
        "outputs": {
            "dev": {
                "type": "bigquery",
                "method": "service-account",
                "keyfile": credentials_location,
                "project": project_id,
                "dataset": dbt_username,
                "threads": 10,
                "timeout_seconds": 300,
                "location": "US",
                "priority": "interactive",
            }
        },
    }

    # update the profiles.yml file
    profile = path.join(path.expanduser("~"), ".dbt", "profiles.yml")
    profile_choice = input(PROFILE_CHOICE_HELPTEXT.format(profile=profile))
    profile = profile_choice or profile
    with open(profile, "r", encoding="utf-8") as f:
        content = f.read()
        profile_yml = yaml.load(content)

    profile_yml[project_id_underscore] = profile_entry
    copy_choice = inplace_or_copy("profile")
    file, extension = path.splitext(profile)
    with open(file + copy_choice + extension, "w", encoding="utf-8") as f:
        yaml.dump(profile_yml, f)
    return credentials_location, dbt_username


def inplace_or_copy(filetype):
    """
    Prompts the user to choose between replacing the existing file or making a copy of the file.

    :param filetype: Type of file to be replaced or copied
    :return: '_copy' if the user wants to make a copy, else an empty string
    """
    choice = ""
    while choice not in ("r", "c"):
        choice = input(INPLACE_OR_COPY_HELPTEXT.format(filename=filetype))
    return "_copy" if choice == "c" else ""


def get_dbt_artifacts_with_version():
    """
    Get the latest version of dbt-artifacts from Github releases API.

    Returns:
    dict: Dictionary containing package name and version in the format:
    {'package': 'brooklyn-data/dbt_artifacts', 'version': 'x.x.x'}
    """
    github_response = requests.get(
        url="https://api.github.com/repos/brooklyn-data/dbt_artifacts/releases",
        timeout=60,
    )
    version: str = github_response.json()[0]["tag_name"]
    return {"package": "brooklyn-data/dbt_artifacts", "version": version}


# TODO test this function and incorporate it into the main script
# def get_dbt_arc_functions_with_version():
#    """
#    Get the latest version of dbt-arc-functions from Github releases API.
#
#    Returns:
#    dict: Dictionary containing package name and revision.
#    """
#    github_response = requests.get(url='https://api.github.com/repos/bsd/dbt-arc-functions/releases', timeout=60)
#    version: str = github_response.json()[0]['tag_name']
# return {'package': 'https://github.com/bsd/dbt-arc-functions.git',
# 'revision': version}


def get_branch_choices():
    """Get the name of the current active branch for the current repository.
    If the current commit is a tag,
    returns the name of the tag.

    Returns:
        str: The name of the current active branch or tag.
    """
    github_response = requests.get(
        url="https://api.github.com/repos/bsd/dbt-arc-functions/releases", timeout=60
    )
    most_recent_release: str = github_response.json()[0]["tag_name"]
    repo = git.Repo(search_parent_directories=True)
    # get the current commit
    current_commit = repo.commit()
    # if the current commit is a tag, return the tag name
    active_branch = next(
        (str(tag) for tag in repo.tags if current_commit == tag.commit),
        repo.active_branch.name,
    )
    return {"most_recent_release": most_recent_release, "active_branch": active_branch}


def write_packages_yml(dbt_packages_path, branch_choices, yaml, dbt_artifacts_choice):
    """
    Write a 'packages.yml' file to the given `dbt_packages_path`
    with the current revision set to the given
    `active_branch_name`. The `yaml` object is
    used to dump the `packages_dict` to the file.
    If a 'packages.yml' file already exists at the given path,
    the user is prompted to confirm whether they want to replace the file.
    """
    packages_dict = packages_dict_template.copy()
    revision_choice = input(
        REVISION_CHOICE_HELPTEXT.format(
            active_branch=branch_choices["active_branch"],
            most_recent_release=branch_choices["most_recent_release"],
        )
    )
    # if the user enters a revision, use that, otherwise use the active branch
    # name
    revision = revision_choice or branch_choices["active_branch"]
    # set the revision for the dbt-arc-functions package
    packages_dict["packages"][1]["revision"] = revision
    # if the user wants to use dbt-artifacts, add it to the packages.yml file
    if dbt_artifacts_choice == "y":
        packages_dict["packages"].append(get_dbt_artifacts_with_version())
    if not path.exists(dbt_packages_path):
        with open(dbt_packages_path, "w", encoding="utf-8") as f:
            yaml.dump(packages_dict, f)
    elif (
        input(f"Would you like to replace packages.yml with:\n{packages_dict}\n(y/n)\n")
        == "y"
    ):
        with open(dbt_packages_path, "w", encoding="utf-8") as f:
            yaml.dump(packages_dict, f)


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
    dbt_project_path = input(
        "Please enter the full path of the dbt_project.yml you'd like to modify:\n"
    )
    # get the path to the dbt project directory
    path.dirname(dbt_project_path)
    # get the project id from the user
    project_id = input(
        "\nPlease enter the name of the Google Project (should look like bsd-projectname):\n"
    )
    # replace dashes with underscores in the project id
    project_id_underscore = project_id.replace("-", "_")
    # initialize the YAML object
    yaml = initialize_yaml()
    # adds dbt_artifacts package to the dbt_project.yml file if the user wants
    # it
    dbt_artifacts_choice = None
    while dbt_artifacts_choice not in ("y", "n"):
        dbt_artifacts_choice = input(DBT_ARTIFACTS_CHOICE_HELPTEXT)
    # update the dbt_project.yml file
    update_dbt_project(
        dbt_project_path, project_id, project_id_underscore, yaml, dbt_artifacts_choice
    )
    # get the path to the dbt project directory
    dbt_base_path = path.dirname(dbt_project_path)
    # get the path to the packages.yml file
    dbt_packages_path = path.join(dbt_base_path, "packages.yml")
    # get the name of the active branch of dbt-arc-functions
    branch_choices = get_branch_choices()
    # write the packages.yml file
    write_packages_yml(dbt_packages_path, branch_choices, yaml, dbt_artifacts_choice)
    # get the path to the models/example folder
    dbt_example_path = path.join(dbt_base_path, "models", "example")
    if (
        path.exists(dbt_example_path)
        and input("\nCan I delete 'models/example'? (y/n)\n") == "y"
    ):
        rmtree(dbt_example_path)
    # update the profile.yml file
    dbt_credentials_path, dbt_username = update_profile_yml(
        project_id, project_id_underscore, yaml
    )
    print("Program terminated successfully!")
    return dbt_project_path, project_id, yaml, dbt_credentials_path, dbt_username


if __name__ == "__main__":
    main()
