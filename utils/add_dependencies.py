import subprocess
import click
from os import path
import re

def check_dbt_installed():
    try:
        subprocess.run(["dbt", "--version"], capture_output=True, check=True)
        return True
    except:
        return False

@click.command()
@click.option('--dbt_base_path', default='', help='The base directory of your dbt project as an absolute path')
def main(dbt_base_path):
    if not check_dbt_installed():
        click.echo("dbt is not installed. Please install dbt before running this script.")
        return
    if not dbt_base_path:
        dbt_base_path = click.prompt("Please enter the base directory of your dbt project as an absolute path")
    bash_command = f"cd {dbt_base_path} ;dbt run"
    matches = [["dependency", "filename"]]
    process = subprocess.run(f"cd {dbt_base_path} ;dbt deps", capture_output=True, shell=True)
    output = process.stdout.decode()
    click.echo(output)
    run_count = 0
    while matches and run_count < 10:
        run_count += 1
        try:
            process = subprocess.run(bash_command, capture_output=True, shell=True)
            output = process.stdout.decode()
            matches = re.findall(r"(-- depends_on: .*?}}).*?called by model [a-z_]+ \((.*?)\)", output, re.DOTALL)
            for dependency, filename in matches:
                with open(path.join(dbt_base_path, filename), 'r') as f:
                    content = f.read()
                with open(path.join(dbt_base_path, filename), 'w') as f:
                    content = dependency + '\n' + content
                    f.write(content)
                    click.echo(f"\nUpdated the file: {filename}\nWith: {dependency}")
            if not matches:
                matches = re.findall(r'Syntax error: Expected "\(" or keyword SELECT or keyword WITH but got ";"', output)
                if matches: click.echo("\nWe have to run again to process some intermediate table builds.\n")
            if run_count >= 10:
                click.echo("\nThis program will break now because dbt run has happened 10 times and we're still getting errors.\n")
        except:
            subprocess.CalledProcessError as e:
            if "Could not find profile" in e.stderr.decode():
                click.echo("Could not find profile named. Please check if you have a dbt profile installed locally.")
                return
            else:
                raise e
    click.echo(output)
        

if __name__ == '__main__':
    main()
