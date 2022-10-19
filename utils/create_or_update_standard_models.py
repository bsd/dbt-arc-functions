#!/usr/bin/env python3
# coding: utf-8
# TODO add function which checks if documentation exists for current sql
# TODO add docstrings for each function
import os
import re
from os import path
import difflib

dbt_string = """-- macro used to create this file can be found at:
-- {github_path}
{{{{ dbt_arc_functions.{function} }}}}
"""


def get_create_or_update():
    print("\nFirst thing's first, do you want to create a new set of standard models or update an existing set?")
    create = None
    while create not in ('c', 'u'):
        print("Enter c to (c)reate new standard models or u to (u)pdate standard models.")
        create = input()
    return create == 'c'


def get_destination():
    print("\nNow, what's the absolute or relative path where you'd like to create models?")
    print("This is usually the 'models' folder of the target repo.")
    print("This script will not let you create models in the current repo.")
    destination_path = ''
    while not destination_path and not (destination_path.endswith(path.join('..', 'models'))
                                        or destination_path.endswith(path.join('dbt-arc-functions', 'models'))):
        print("\nPlease enter the absolute or relative path where you'd like to create standard models:")
        destination_path = input()
    return destination_path


def get_list_of_sources(macros_path):
    list_of_sources = [directory for directory in os.listdir(macros_path) if path.isdir(path.join(macros_path, directory))]
    return list_of_sources


def get_sources_wanted(list_of_sources):
    print("\nWhich set of standard models would you like to use in this dbt project? Here's the list:")
    [print(source) for source in list_of_sources]
    print("\nYou can just add one or input a comma separated list (no brackets or quotes necessary): ")
    sources_wanted = input()
    print()
    sources_wanted = list(set([source.strip() for source in sources_wanted.split(',')]))
    return sources_wanted


def overwrite_choice(source_file_path, output, destination_file_path):
    with open(destination_file_path, 'r') as d:
        d_text = d.readlines()
        differences = list(difflib.unified_diff(
            d_text, output, fromfile=destination_file_path,
            tofile=source_file_path, lineterm='\n'))
        if not differences:
            print(f"No differences between {destination_file_path} and {source_file_path}, skipping.")
            return False
        print(
            f'\nThere are differences between your model at:\n{destination_file_path} and the standard model at:\n{source_file_path}')
        print("Here is your current model:\n")
        print(*d_text, sep='')
        print("Here is the standard model in arc-dbt-functions:\n")
        print(*output, sep='')
        print("Here are the differences between the two files: \n")
        print(*differences, sep='')
    overwrite = None
    while overwrite not in ('o', 'k'):
        print("Enter o to (o)verwrite your model with the standard model or k to (k)eep your model.")
        overwrite = input()
    return overwrite == 'o'


def extract_dependencies(output, destination_file_path, dependencies_regex):
    with open(destination_file_path, 'r') as d:
        dependencies = dependencies_regex.findall(d.read())
    if not dependencies: return output
    print("Would you like to keep these dependencies?")
    print(*dependencies)
    add_dependencies = None
    while add_dependencies not in ('a', 'i'):
        print("Enter a to (a)dd dependencies or i to (i)gnore them.")
        add_dependencies = input()
    if add_dependencies == 'a':
        for dependency in dependencies:
            output = dependency + output
    return output


def write_new_file_choice(output, destination_file_path):
    print(f"\nYou do not have a model at {destination_file_path}\n")
    print("Here is the standard model in arc-dbt-functions:\n")
    print(*output, sep='')
    print(f"Would you like to create a model with this content at {destination_file_path}?\n")
    yes = None
    while yes not in ('y', 'n'):
        print("Enter y for (y)es or n for (n)o:")
        yes = input()
    return yes == 'y'


def process_sources(sources_wanted, list_of_sources, macros_path, create, destination):
    """
    :param sources_wanted: list of standard models that a client wants to copy to their dbt repo
    :param list_of_sources: list of available standard models within dbt-arc-functions
    :param macros_path: path of the macros folders
    :param create: whether a client wants to create standard models (false if they want to update)
    :param destination: destination dbt repo where client would like to create/update standard models
    :return: None
    """
    # TODO this function is very large and therefore hard to parse, break into smaller chunks
    sources_path = path.join('..', 'sources')
    model_types = [sources_path, 'staging', 'marts']
    rx = re.compile(r"\{%\s+macro\s+(.*?)\s+%\}", re.DOTALL)
    dependencies_regex = re.compile(r"--\s+depends_on:\s+.*\n")
    git_prepend = "https://github.com/bsd/dbt-arc-functions/blob/main/macros"
    for source in sources_wanted:
        if source not in list_of_sources:
            print(f'Sorry, {source} is not in the list of sources above')
            continue
        list_of_models = os.listdir(path.join(macros_path, source))
        list_of_models.append(sources_path)
        for model_type in list_of_models:
            # TODO break this out into separate function
            if model_type not in model_types:
                print(f"Weird, {model_type} is not one of our standard model types. Going to skip.")
                continue
            source_path = sources_path if model_type == sources_path else path.join(macros_path, source, model_type)
            destination_path = path.join(destination, model_type, source) if model_type != sources_path else path.join(
                destination, 'sources')
            if path.exists(destination_path) and create and not destination_path.endswith('sources'):
                print("\nThis directory: {} already exists and I don't want to overwrite anything in create mode."
                      .format(destination_path))
                continue
            elif not path.exists(destination_path) and not create:
                print("\nThis directory: {} doesn't exist and I don't want to make directories in update mode."
                      .format(destination_path))
                continue
            elif create and not path.exists(destination_path):
                os.makedirs(destination_path)
            for _, _, files in os.walk(source_path):
                # TODO break this out into separate function
                for file in files:
                    if not (file.endswith('.sql') or file == f"{source}.yml"):
                        continue
                    source_file_path = path.join(source_path, file)
                    with open(source_file_path, 'r') as f:
                        destination_file_path = path.join(destination_path, file)
                        docs_file_path = source_path.replace('macros', 'documentation').replace('sql', 'yml')
                        content = f.read()
                        if file.endswith('.sql'):
                            function = rx.search(content).group(1) if rx.search(content) else None
                            github_path = '/'.join([git_prepend, source, model_type, file])
                            output = dbt_string.format(github_path=github_path,
                                                       function=function)
                        else:
                            output = content
                        if path.exists(destination_file_path) and not create:
                            output_formatted = [line + ('\n' if i < len(output.split('\n')) else '') for i, line in
                                                enumerate(output.split('\n'))]
                            if file.endswith('.sql'): output_formatted = output_formatted[:-1]
                            else: output_formatted[-1] = output_formatted[-1][:-1]
                            if not overwrite_choice(source_file_path, output_formatted, destination_file_path):
                                continue
                            output = extract_dependencies(output, destination_file_path, dependencies_regex)
                        if not path.exists(destination_file_path) and not create:
                            if not write_new_file_choice(output, destination_file_path):
                                continue
                        if output:
                            with open(destination_file_path, 'w') as tf:
                                tf.writelines(output)
                                print(destination_file_path + " created successfully!")



def main(dbt_models_path=''):
    macros_path = path.join('..', 'macros')
    create = get_create_or_update()
    try:
        if not dbt_models_path:
            dbt_models_path = get_destination()
        list_of_sources = get_list_of_sources(macros_path)
        sources_wanted = get_sources_wanted(list_of_sources)
        process_sources(sources_wanted, list_of_sources, macros_path, create, dbt_models_path)
    except Exception as e:
        print(e)
        pass
    print('\nProgram terminated successfully!\n')

if __name__ == '__main__':
    main()
