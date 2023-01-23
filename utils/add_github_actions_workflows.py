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

Third, you need to set up an environment in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-a-deployment-environment

Fourth, you need to create a job in dbt Cloud. Here are docs:
https://docs.getdbt.com/docs/get-started/getting-started/building-your-first-project/schedule-a-job#create-and-run-a-job

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


def create_dbt_rom_yml_from_template(
        dbt_rom_yml_template, dbt_cloud_api_run_url):
    dbt_rom_yml = deepcopy(dbt_rom_yml_template)
    dbt_rom_yml['jobs']['dbt_run']['steps'][0]['run'] = dbt_rom_yml['jobs']['dbt_run']['steps'][0]['run'].replace('{dbt_cloud_api_run_url}',
                                                                                                                  dbt_cloud_api_run_url
                                                                                                                  )
    return dbt_rom_yml


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


def main(dbt_base_path='', yaml=None):
    if not dbt_base_path:
        dbt_base_path = get_dbt_base_path()
    if not yaml:
        yaml = initialize_yaml()
    dbt_rom_filename = 'dbt_run_on_merge.yml'
    dbt_rom_yml_path = os.path.join('yml_files', dbt_rom_filename)
    dbt_rom_yml_template = load_workflow_yml(dbt_rom_yml_path, yaml)
    input(starting_prompt)
    input(prompt_api_added_to_secrets)
    dbt_cloud_job_url = input(prompt_dbt_cloud_job_url)
    dbt_cloud_api_run_url = convert_cloud_job_url_to_api_run_url(
        dbt_cloud_job_url)
    dbt_rom_yml = create_dbt_rom_yml_from_template(
        dbt_rom_yml_template, dbt_cloud_api_run_url)
    dbt_rom_path = create_workflow_path_and_folders(
        dbt_base_path, dbt_rom_filename)
    write_workflow(dbt_rom_path, dbt_rom_yml, yaml)
    print("\nWorkflow successfully created!")


if __name__ == '__main__':
    main()
