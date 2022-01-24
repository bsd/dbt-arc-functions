#!/usr/bin/env python3
# coding: utf-8

import re
import subprocess
from os import path


def main(dbt_base_path=""):
    if not dbt_base_path:
        dbt_base_path = input("Please enter the base directory of your dbt project as an absolute path:\n")
    bash_command = f"cd {dbt_base_path} ;dbt run"
    matches = [["dependency", "filename"]]
    process = subprocess.run(f"cd {dbt_base_path} ;dbt deps", capture_output=True, shell=True)
    output = process.stdout.decode()
    print(output)
    run_count = 0
    while matches and run_count < 10:
        run_count += 1
        process = subprocess.run(bash_command, capture_output=True, shell=True)
        output = process.stdout.decode()
        matches = re.findall(r"(-- depends_on: .*?}}).*?called by model [a-z_]+ \((.*?)\)", output, re.DOTALL)
        for dependency, filename in matches:
            with open(path.join(dbt_base_path, filename), 'r') as f:
                content = f.read()
            with open(path.join(dbt_base_path, filename), 'w') as f:
                content = dependency + '\n' + content
                f.write(content)
                print(f"\nUpdated the file: {filename}\nWith: {dependency}")
        if not matches:
            matches = re.findall(r'Syntax error: Expected "\(" or keyword SELECT or keyword WITH but got ";"', output)
            if matches: print("\nWe have to run again to process some intermediate table builds.\n")
        if run_count >= 10:
            print("\nThis program will break now because dbt run has happened 10 times and we're still getting errors.\n")
    print(output)


if __name__ == '__main__':
    main()
