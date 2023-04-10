import ruamel.yaml
import subprocess
import click
import os


def initialize_yaml():
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 5000
    return yaml


def check_dbt_installed():
    try:
        subprocess.run(["dbt", "--version"], capture_output=True, check=False)
    except subprocess.CalledProcessError as e:
        click.echo(
            "dbt is not installed. Please install dbt before running this script.")
        raise e


def check_profiles_file():
    if not os.path.exists(os.path.expanduser('~/.dbt/profiles.yml')):
        click.echo(
            "Could not find the profiles.yml file in the ~/.dbt/ directory.\
            Please check that it exists.")
        raise FileNotFoundError


def run_dbt_subprocess(bash_command: str) -> str:
    try:
        process = subprocess.run(
            bash_command, capture_output=True, shell=True, check=False)
        return process.stdout.decode()
    except subprocess.CalledProcessError as called_process_error:
        if "Could not find profile named" in called_process_error.stderr.decode():
            click.echo(
                "Could not find dbt profile named [name of the profile]. "
                "Please check if you have a dbt profile installed locally for this project.")
            return
        raise called_process_error
