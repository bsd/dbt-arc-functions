import os
import re
from copy import deepcopy

import ruamel.yaml

starting_prompt = """You'll need to do some setup to have this util run.

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
prompt_api_added_to_secrets = """Please add a secret to your dbt repo in Github with the name DBTCLOUDAPIKEY
Instructions on how to do this below:
https://docs.github.com/en/codespaces/managing-codespaces-for-your-organization/managing-encrypted-secrets-for-your-repository-and-organization-for-github-codespaces

Please press return when you've done this.
"""
prompt_dbt_cloud_job_url = """Now please go to your cloud job in dbt Cloud, copy the URL, paste it here, and press return.
It should look something like this: https://cloud.getdbt.com/next/deploy/1713/projects/150080/jobs/121379
"""
prompt_create_environment_and_job = """Now we're going to create a {environment} workflow. This will be run everytime you {trigger}.

First, you need to set up an {environment} environment in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-a-deployment-environment

Give it the name {environment}, and set the Environment Type to Deployment. Set dataset to {environment}.

Now, you need to create a job in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-and-run-a-job

Set the Job Name to {environment}. 
The main thing you might want to add here is a Schedule to run on, especially if this is prod.
This is all the way at the bottom, under Triggers.
"""
merge_string="""push:
    branches: [ main ]
"""
pr_string="""pull_request:"""

def get_dbt_base_path():
    return input(
        "Please enter the base directory of your dbt project as an absolute path:\n")


def initialize_yaml():
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    return yaml


def load_workflow_yml(workflow_yml_path, yaml):
    with open(workflow_yml_path, 'r') as f:
        content = f.read()
        workflow_yml = yaml.load(content)
    return workflow_yml


def convert_cloud_job_url_to_api_run_url(dbt_cloud_job_url):
    dbt_cloud_job_url = dbt_cloud_job_url.replace("next/", "")
    dbt_cloud_job_url = dbt_cloud_job_url.replace(
        "deploy/", "api/v2/accounts/")
    dbt_cloud_api_run_url = re.sub(
        r"projects/\d+/", "", dbt_cloud_job_url) + "/run/"
    return dbt_cloud_api_run_url


def create_dbt_run_yml_from_template(
        dbt_run_yml_template, dbt_cloud_api_run_url,dbt_run_yml,trigger_string):
    dbt_run_yml = deepcopy(dbt_run_yml_template)
    dbt_run_yml['jobs']['dbt_run']['steps'][0]['run'] = dbt_run_yml['jobs']['dbt_run']['steps'][0]['run'].replace('{dbt_cloud_api_run_url}',
                                                                                                                  dbt_cloud_api_run_url
                                                                                                                  )
    dbt_run_yml['on']=
    return dbt_run_yml


def create_workflow_path_and_folders(dbt_base_path, workflow_filename):
    github_folder_path = os.path.join(dbt_base_path, ".github")
    if not os.path.exists(github_folder_path):
        os.mkdir(github_folder_path)
    workflow_folder_path = os.path.join(github_folder_path, "workflows")
    if not os.path.exists(workflow_folder_path):
        os.mkdir(workflow_folder_path)
    workflow_path = os.path.join(workflow_folder_path, workflow_filename)
    return workflow_path


def write_workflow(workflow_path, workflow_yml, yaml):
    with open(workflow_path, "w") as f:
        yaml.dump(workflow_yml, f)

def create_dbt_run_workflow(environment,trigger,dbt_base_path,yaml):
    input(prompt_create_environment_and_job.format(environment=environment,trigger=trigger))
    dbt_run_filename = 'dbt_run_on_trigger.yml'
    dbt_run_yml_path = os.path.join('yml_files', dbt_run_filename)
    dbt_run_yml_template = load_workflow_yml(dbt_run_yml_path, yaml)
    dbt_cloud_job_url = input(prompt_dbt_cloud_job_url)
    dbt_cloud_api_run_url = convert_cloud_job_url_to_api_run_url(
        dbt_cloud_job_url)
    dbt_run_yml = create_dbt_run_yml_from_template(
        dbt_run_yml_template, dbt_cloud_api_run_url)
    dbt_trigger_filename = dbt_run_filename.replace('trigger',trigger)
    dbt_run_path = create_workflow_path_and_folders(
        dbt_base_path, dbt_trigger_filename)
    write_workflow(dbt_run_path, dbt_run_yml, yaml)
    print("\nWorkflow successfully created!")

def main(dbt_base_path='', yaml=None):
    if not dbt_base_path:
        dbt_base_path = get_dbt_base_path()
    if not yaml:
        yaml = initialize_yaml()
    input(starting_prompt)
    input(prompt_api_added_to_secrets)
    create_dbt_run_workflow('prod','merge',dbt_base_path,yaml)    


if __name__ == '__main__':
    main()
