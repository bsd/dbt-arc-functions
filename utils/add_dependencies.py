#!/usr/bin/env python3
"""This script will add the dependencies to the models in your dbt project."""

import subprocess
import click
from os import path
import re
from utils import check_dbt_installed, check_profiles_file, run_dbt_subprocess
















def get_no_version_check_choice() -> str:
    """This function asks user if they want to run dbt without checking version."""
    print(
        "Do you want dbt to run without checking dbt version? This can help if you are getting version errors."
    )
    return (
        " --no-version-check"
        if input("Enter 'y' to run w.o version check, else (return):\n") == "y"
        else ""
    )


def main(dbt_base_path=None):
    """This function writes dependency strings to the top of dbt models.

    Args:
        dbt_base_path (path): the path to your dbt project locally

    Raises:
        called_process_error: an error is called if you do not have the dbt project installed correctly
    """
    check_dbt_installed()
    check_profiles_file()
    if not dbt_base_path:
        dbt_base_path = click.prompt(
            "Please enter the base directory of your dbt project as an absolute path"
        )
    no_version_check_choice = get_no_version_check_choice()
    bash_command = f"cd {dbt_base_path} ;dbt run{no_version_check_choice}"
    matches = [["dependency", "filename"]]
    process = subprocess.run(
        f"cd {dbt_base_path} ;dbt deps", capture_output=True, shell=True, check=False
    )
    output = process.stdout.decode()
    click.echo(output)
    run_count = 0
    while matches:
        if run_count > 10:
            click.echo(
                "\nThis program will break now because dbt run has happened\
                10 times and we're still getting errors.\n"
            )
            break
        run_count += 1
        output = run_dbt_subprocess(bash_command)
        matches = re.findall(
            r"(-- depends_on: .*?}}).*?called by model [a-z_]+ \((.*?)\)",
            output,
            re.DOTALL,
        )
        for dependency, filename in matches:
            with open(path.join(dbt_base_path, filename), "r") as f:
                content = f.read()
            with open(path.join(dbt_base_path, filename), "w") as f:
                content = dependency + "\n" + content
                f.write(content)
                click.echo(f"\nUpdated the file: {filename}\nWith: {dependency}")
        if not matches:
            matches = re.findall(
                r'Syntax error: Expected "\(" or keyword SELECT or keyword WITH but got ";"',
                output,
            )
            if matches:
                click.echo(
                    "\nWe have to run again to process some intermediate table builds.\n"
                )
    click.echo(output)


@click.command()
@click.option(
    "--dbt_base_path",
    default="",
    help="The base directory of your dbt project as an absolute path",
)
def command_line_main(dbt_base_path):
    main(dbt_base_path)


if __name__ == "__main__":
    command_line_main()
