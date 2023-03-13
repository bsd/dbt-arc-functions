"""This script will add the dependencies to the models in your dbt project."""

# TODO to add_dependencies for no version check

import subprocess
import click
from os import path
import re


def check_dbt_installed():
    try:
        subprocess.run(["dbt", "--version"], capture_output=True, check=False)
    except subprocess.CalledProcessError as e:
        click.echo(
            "dbt is not installed. Please install dbt before running this script.")
        raise e


def check_profiles_file():
    if not path.exists(path.expanduser('~/.dbt/profiles.yml')):
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


def main(dbt_base_path=None):
    """ This function writes dependency strings to the top of dbt models.

    Args:
        dbt_base_path (path): the path to your dbt project locally

    Raises:
        called_process_error: an error is called if you do not have the dbt project installed correctly
    """
    check_dbt_installed()
    check_profiles_file()
    if not dbt_base_path:
        dbt_base_path = click.prompt(
            "Please enter the base directory of your dbt project as an absolute path")
    bash_command = f"cd {dbt_base_path} ;dbt run"
    matches = [["dependency", "filename"]]
    process = subprocess.run(
        f"cd {dbt_base_path} ;dbt deps",
        capture_output=True,
        shell=True,
        check=False)
    output = process.stdout.decode()
    click.echo(output)
    run_count = 0
    while matches:
        if run_count > 10:
            click.echo(
                "\nThis program will break now because dbt run has happened\
                10 times and we're still getting errors.\n")
            break
        run_count += 1
        output = run_dbt_subprocess(bash_command)
        matches = re.findall(
            r"(-- depends_on: .*?}}).*?called by model [a-z_]+ \((.*?)\)",
            output,
            re.DOTALL)
        for dependency, filename in matches:
            with open(path.join(dbt_base_path, filename), 'r') as f:
                content = f.read()
            with open(path.join(dbt_base_path, filename), 'w') as f:
                content = dependency + '\n' + content
                f.write(content)
                click.echo(
                    f"\nUpdated the file: {filename}\nWith: {dependency}")
        if not matches:
            matches = re.findall(
                r'Syntax error: Expected "\(" or keyword SELECT or keyword WITH but got ";"',
                output)
            if matches:
                click.echo(
                    "\nWe have to run again to process some intermediate table builds.\n")
    click.echo(output)


if __name__ == '__main__':
    main()
