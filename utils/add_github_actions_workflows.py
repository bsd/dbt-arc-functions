""" This script will add a dbt run workflow to your dbt repo in Github. """

import os
import re
from copy import deepcopy

import ruamel.yaml

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
    'prod': {'trigger': 'merge', 'trigger_string': MERGE_STRING},
    'dev': {'trigger': 'pr', 'trigger_string': PR_STRING}
}


def get_dbt_base_path():
    """ This function gets the dbt_base_path in the user's computer
    """
    return input(
        "Please enter the base directory of your dbt project as an absolute path:\n")


def initialize_yaml():
    """ this function initializes yaml """
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    return yaml


def load_workflow_yml(workflow_yml_path, yaml):
    """ this function takes the path to the github actions workflow and returns a formatted yaml """
    with open(workflow_yml_path, 'r') as f:
        content = f.read()
        workflow_yml = yaml.load(content)
    return workflow_yml


def convert_cloud_job_url_to_api_run_url(dbt_cloud_job_url):
    """_summary_

    Args:
        dbt_cloud_job_url (_type_): _description_

    Returns:
        _type_: _description_
    """
    dbt_cloud_job_url = dbt_cloud_job_url.replace("next/", "")
    dbt_cloud_job_url = dbt_cloud_job_url.replace(
        "deploy/", "api/v2/accounts/")
    return re.sub(r"projects/\d+/", "", dbt_cloud_job_url) + "/run/"


def create_dbt_run_yml_from_template(
        dbt_run_yml_template, dbt_cloud_api_run_url, trigger_yml):
    """_summary_

    Args:
        dbt_run_yml_template (_type_): _description_
        dbt_cloud_api_run_url (_type_): _description_
        trigger_yml (_type_): _description_

    Returns:
        _type_: _description_
    """
    dbt_run_yml = deepcopy(dbt_run_yml_template)
    dbt_run_yml['jobs']['dbt_run']['steps'][0]['run'] = dbt_run_yml['jobs']['dbt_run']['steps'][0]['run'].replace('{dbt_cloud_api_run_url}',
                                                                                                                  dbt_cloud_api_run_url
                                                                                                                  )
    dbt_run_yml['on'] = trigger_yml
    return dbt_run_yml


def create_workflow_path_and_folders(dbt_base_path, workflow_filename):
    """_summary_

    Args:
        dbt_base_path (_type_): _description_
        workflow_filename (_type_): _description_

    Returns:
        _type_: _description_
    """
    github_folder_path = os.path.join(dbt_base_path, ".github")
    if not os.path.exists(github_folder_path):
        os.mkdir(github_folder_path)
    workflow_folder_path = os.path.join(github_folder_path, "workflows")
    if not os.path.exists(workflow_folder_path):
        os.mkdir(workflow_folder_path)
    return os.path.join(workflow_folder_path, workflow_filename)


def write_workflow(workflow_path, workflow_yml, yaml):
    """_summary_

    Args:
        workflow_path (_type_): _description_
        workflow_yml (_type_): _description_
        yaml (_type_): _description_
    """
    with open(workflow_path, "w") as f:
        yaml.dump(workflow_yml, f)


def create_dbt_run_workflow(environment, dbt_base_path, yaml):
    """_summary_

    Args:
        environment (_type_): _description_
        dbt_base_path (_type_): _description_
        yaml (_type_): _description_
    """
    environment_dict = trigger_dict[environment]
    trigger, trigger_string = environment_dict['trigger'], environment_dict['trigger_string']
    input(prompt_create_environment_and_job.format(
        environment=environment, trigger=trigger))
    dbt_run_filename = 'dbt_run_on_trigger.yml'
    dbt_run_yml_path = os.path.join('yml_files', dbt_run_filename)
    dbt_run_yml_template = load_workflow_yml(dbt_run_yml_path, yaml)
    dbt_cloud_job_url = input(prompt_dbt_cloud_job_url)
    dbt_cloud_api_run_url = convert_cloud_job_url_to_api_run_url(
        dbt_cloud_job_url)
    trigger_yml = yaml.load(trigger_string)
    dbt_run_yml = create_dbt_run_yml_from_template(
        dbt_run_yml_template, dbt_cloud_api_run_url, trigger_yml)
    dbt_trigger_filename = dbt_run_filename.replace('trigger', trigger)
    dbt_run_path = create_workflow_path_and_folders(
        dbt_base_path, dbt_trigger_filename)
    write_workflow(dbt_run_path, dbt_run_yml, yaml)
    print("\n*** Workflow successfully created! ***\n")


def main(dbt_base_path='', yaml=None):
    """_summary_

    Args:
        dbt_base_path (str, optional): _description_. Defaults to ''.
        yaml (_type_, optional): _description_. Defaults to None.
    """
    if not dbt_base_path:
        dbt_base_path = get_dbt_base_path()
    if not yaml:
        yaml = initialize_yaml()
    input(starting_prompt)
    input(prompt_api_added_to_secrets)
    create_dbt_run_workflow('prod', dbt_base_path, yaml)
    create_dbt_run_workflow('dev', dbt_base_path, yaml)
    print("\n***add_github_actions_workflows.py terminated successfully!***\n")


if __name__ == '__main__':
    main()
