import ruamel.yaml
import os
import re

starting_prompt = """You'll need to do some setup to have this util run.

First, you'll need to have a working dbt Github Repo.

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
"""

def initialize_yaml():
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    return yaml

def load_workflow_yml(workflow_yml_path,yaml):
    with open(workflow_yml_path,'r') as f:
        content = f.read()
        workflow_yml = yaml.load(content)
    return workflow_yml

if __name__ == '__main__':
    if not dbt_base_path:
        dbt_base_path = get_destination()
    if not yaml:
        yaml = initialize_yaml()
    dbt_rom_yml_path = os.path.join('yml_files','dbt_run_on_merge.yml')
    dbt_rom_yml = load_workflow_yml(dbt_rom_yml_path,yaml)