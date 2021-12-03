#!/usr/bin/env python3
# coding: utf-8

import subprocess
import re
from os import path

base_dir = input("Please enter the base directory of your dbt project as an absolute path:\n")
bashCommand = f"cd {base_dir} ;dbt run"
matches = [["dependency", "filename"]]
output = "Something didn't work"
while matches:
    process = subprocess.run(bashCommand, capture_output=True, shell=True)
    output = process.stdout.decode()
    matches = re.findall(r"(-- depends_on: .*?}}).*?called by model [a-z_]+ \((.*?)\)", output, re.DOTALL)
    for dependency, filename in matches:
        with open(path.join(base_dir, filename), 'r') as f:
            content = f.read()
        with open(path.join(base_dir, filename), 'w') as f:
            content = dependency + '\n' + content
            f.write(content)
            print(f"\nUpdated the file: {filename}\nWith: {dependency}")
print(output)
