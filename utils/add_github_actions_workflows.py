""" This script will add a dbt run workflow to your dbt repo in Github. """

import os
import re
from copy import deepcopy

from utils import initialize_yaml

STARTING_PROMPT = """You'll need to do some setup to have this util run.

First, you'll need to have a working dbt repo in Github.

Second, you'll need to have a dbtCloud project set up.
If you've never done this before, it's worth talking to an engineer.
Here are the docs if you'd like to try on your own:
https://docs.getdbt.com/docs/get-started/getting-started/set-up-dbt-cloud

Finally, you'll need your dbt Cloud API key.
You can find it at the bottom of this page:
https://cloud.getdbt.com/next/settings/profile

Please press return when you've done all of this.
"""
PROMPT_API_ADDED_TO_SECRETS = """Please add a secret to your dbt repo\
    in Github with the name DBTCLOUDAPIKEY\
Instructions on how to do this below:
https://docs.github.com/en/codespaces/managing-codespaces-for-your-organization/managing-encrypted-secrets-for-your-repository-and-organization-for-github-codespaces

Please press return when you've done this.
"""
PROMPT_DBT_CLOUD_JOB_URL = """Now please go to your cloud job\
    in dbt Cloud, copy the URL, paste it here, and press return.
It should look something like this:\
    https://cloud.getdbt.com/next/deploy/1713/projects/150080/jobs/121379
"""
PROMPT_CREATE_ENVIRONMENT_AND_JOB = """Now we're going to create a {environment} workflow.\
    This will be run everytime you {trigger}.

First, you need to set up an {environment} environment in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-a-deployment-environment

Give it the name {environment}, and set the Environment Type to Deployment.\
    Set dataset to {environment}.

Now, you need to create a job in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-and-run-a-job

Set the Job Name to {environment}.
Set the Environment to {environment}.
You might additionally want to add a Schedule to run on,\
especially if this is prod.
This is all the way at the bottom, under Triggers.
Hit Save.
"""
MERGE_STRING = """push:
    branches: [ main ]
"""
PR_STRING = """pull_request:"""

TRIGGER_DICT = {
    "prod": {"trigger": "merge", "trigger_string": MERGE_STRING},
    "dev": {"trigger": "pr", "trigger_string": PR_STRING},
}


def get_dbt_base_path():
    """This function prompts the user to enter the base directory
    of their dbt project as an absolute path and returns it."""
    return input(
        "Please enter the base directory of your dbt project as an absolute path:\n"
    )


def load_workflow_yml(workflow_yml_path, yaml):
    """This function takes the path to the github actions workflow and returns a formatted yaml"""
    with open(workflow_yml_path, "r") as f:
        content = f.read()
        workflow_yml = yaml.load(content)
    return workflow_yml


def convert_cloud_job_url_to_api_run_url(dbt_cloud_job_url):
    """
    Converts a dbt Cloud job URL to a URL that can be used to trigger a dbt Cloud API run.

    Args:
        dbt_cloud_job_url (str): A string representing the URL of a dbt Cloud job.
        The URL should have the following format:
        https://cloud.getdbt.com/<account_name>/deploy/<deployment_id>/projects/
            <project_id>/jobs/<job_id>

    Returns:
        str: A string representing the URL of a dbt Cloud API run that
        can be triggered using the URL returned by this method.
        The URL should have the following format:
        https://cloud.getdbt.com/api/v2/accounts/<account_id>/runs/<run_id>/
    """
    dbt_cloud_job_url = dbt_cloud_job_url.replace("next/", "")
    dbt_cloud_job_url = dbt_cloud_job_url.replace("deploy/", "api/v2/accounts/")
    return re.sub(r"projects/\d+/", "", dbt_cloud_job_url) + "/run/"


def create_dbt_run_yml_from_template(
    dbt_run_yml_template, dbt_cloud_api_run_url, trigger_yml
):
    """
    Creates a new dbt run YAML configuration file by copying a template YAML file
    and replacing placeholders with actual values.

    Args:
        dbt_run_yml_template: A dictionary that represents the dbt run YAML configuration template.
        dbt_cloud_api_run_url: A string that represents the URL
            of the dbt Cloud job run API endpoint.
        trigger_yml: A dictionary that represents the YAML configuration
            for the GitHub Actions trigger.

    Returns:
        A dictionary that represents the new dbt run YAML configuration file
        with placeholders replaced with actual values.

    Raises:
        TypeError: If any of the input arguments has an invalid type.
        KeyError: If any of the required keys is missing from the input dictionary.

    Examples:
        dbt_run_yml_template = {'jobs': {'dbt_run':
                                {'steps': [{'run': 'dbt run --models source:table1+table2'}]}}}
        dbt_cloud_api_run_url =
            'https://cloud.getdbt.com/api/v2/accounts/1234/projects/acme/jobs/5678/runs/91011/'
        trigger_yml = {'push': {'branches': ['main']}}
        dbt_run_yml = create_dbt_run_yml_from_template(dbt_run_yml_template, dbt_cloud_api_run_url, trigger_yml)
    """
    dbt_run_yml = deepcopy(dbt_run_yml_template)
    dbt_run_yml["jobs"]["dbt_run"]["steps"][0]["run"] = dbt_run_yml["jobs"]["dbt_run"][
        "steps"
    ][0]["run"].replace("{dbt_cloud_api_run_url}", dbt_cloud_api_run_url)
    dbt_run_yml["on"] = trigger_yml
    return dbt_run_yml


def create_workflow_path_and_folders(dbt_base_path, workflow_filename):
    """
    Creates the necessary folder structure for a GitHub Actions workflow file and returns its path.

    Args:
        dbt_base_path (str): The base path of the dbt project.
        workflow_filename (str): The filename of the workflow file.

    Returns:
        str: The full path to the workflow file, including its filename.
    """
    github_folder_path = os.path.join(dbt_base_path, ".github")
    if not os.path.exists(github_folder_path):
        os.mkdir(github_folder_path)
    workflow_folder_path = os.path.join(github_folder_path, "workflows")
    if not os.path.exists(workflow_folder_path):
        os.mkdir(workflow_folder_path)
    return os.path.join(workflow_folder_path, workflow_filename)


def write_workflow(workflow_path, workflow_yml, yaml):
    """
    Write the workflow_yml to the specified workflow_path using the provided yaml library.

    Args:
        workflow_path (str): The file path to write the workflow_yml to.
        workflow_yml (dict): A dictionary containing the contents of the GitHub Actions workflow.
        yaml (obj): An instance of the yaml library to use for dumping the workflow_yml.

    Returns:
        None.
    """
    with open(workflow_path, "w") as f:
        yaml.dump(workflow_yml, f)


def create_dbt_run_workflow(environment, dbt_base_path, yaml):
    """Create a GitHub workflow YAML file for running a DBT job on a trigger.

    Args:
        environment (str): The environment to create the workflow for.
            Must be one of the keys in the `trigger_dict` dictionary.
        dbt_base_path (str): The base path of the DBT project.
        yaml (yaml): A YAML instance used to parse and dump YAML data.

    Returns:
        None
    """
    environment_dict = TRIGGER_DICT[environment]
    trigger, trigger_string = (
        environment_dict["trigger"],
        environment_dict["trigger_string"],
    )
    input(
        PROMPT_CREATE_ENVIRONMENT_AND_JOB.format(
            environment=environment, trigger=trigger
        )
    )
    dbt_run_filename = "dbt_run_on_trigger.yml"
    dbt_run_yml_path = os.path.join("yml_files", dbt_run_filename)
    dbt_run_yml_template = load_workflow_yml(dbt_run_yml_path, yaml)
    dbt_cloud_job_url = input(PROMPT_DBT_CLOUD_JOB_URL)
    dbt_cloud_api_run_url = convert_cloud_job_url_to_api_run_url(dbt_cloud_job_url)
    trigger_yml = yaml.load(trigger_string)
    dbt_run_yml = create_dbt_run_yml_from_template(
        dbt_run_yml_template, dbt_cloud_api_run_url, trigger_yml
    )
    dbt_trigger_filename = dbt_run_filename.replace("trigger", trigger)
    dbt_run_path = create_workflow_path_and_folders(dbt_base_path, dbt_trigger_filename)
    write_workflow(dbt_run_path, dbt_run_yml, yaml)
    print("\n*** Workflow successfully created! ***\n")


def main(dbt_base_path="", yaml=None):
    """
    The main() function is the entry point for the script. It prompts the user to start the script,
    initializes the yaml library and calls create_dbt_run_workflow() function
    for both production and development environments to create their respective workflows.
    Finally, it prints a termination message.

    Args:
        dbt_base_path (str, optional): The base path for the dbt project. Defaults to ''.
        yaml (Any, optional): An instance of the yaml library. Defaults to None.
    """
    if not dbt_base_path:
        dbt_base_path = get_dbt_base_path()
    if not yaml:
        yaml = initialize_yaml()
    input(STARTING_PROMPT)
    input(PROMPT_API_ADDED_TO_SECRETS)
    create_dbt_run_workflow("prod", dbt_base_path, yaml)
    create_dbt_run_workflow("dev", dbt_base_path, yaml)
    print("\n***add_github_actions_workflows.py terminated successfully!***\n")


if __name__ == "__main__":
    main()
