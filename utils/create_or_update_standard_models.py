""" This script creates dbt models that call macros in this project,
    in the client project dbt repository. It generates SQL files that match up to the files in
    the macros folder here. """

#!/usr/bin/env python3
# coding: utf-8
# TODO add inline comments to clarify what each function does
# TODO object oriented rewrite with classes for each function

import difflib
import os
import re
import sys
from pathlib import Path
from os import path
import ruamel.yaml

DBT_STRING = """-- macro used to create this file can be found at:
-- {github_path}
{{{{ dbt_arc_functions.{function} }}}}
"""


def get_create_or_update():
    """Get a boolean indicating whether to create or update standard models.
    Prompts the user to enter 'c' to create new standard models or 'u' to update standard models.
        If the user enters an invalid input, the function will continue to prompt
            until a valid input is provided.
    Returns:
        A boolean indicating whether to create (True) or update (False) standard models.
    """
    print("\nDo you want to create a new set of standard models or update an existing set?")
    create = None
    while create not in ('c', 'u'):
        print("Enter c to (c)reate new standard models or u to (u)pdate standard models.")
        create = input()
    return create == 'c'


def get_destination():
    """Get the absolute or relative path to create models.

    Prompts the user to enter the absolute or relative path
    where they would like to create models. The function
    will continue to prompt until a valid path is provided.

    Returns:
        The absolute or relative path where the models will be created.
    """
    print("\nNow, what's the absolute or relative path where you'd like to create models?")
    print("This is usually the 'models' folder of the target repo.")
    print("This script will not let you create models in the current repo.")
    destination_path = ''
    while (
        not destination_path
        and not destination_path.endswith(path.join('..', 'models'))
        and not destination_path.endswith(
            path.join('dbt-arc-functions', 'models')
        )
    ):
        print("\nPlease enter the absolute or relative path where you'd like to create standard models:")
        destination_path = input()
    return destination_path


def get_list_of_sources(macros_path):
    """Get a list of sources in the specified macros path.

    Args:
        macros_path: The path to the dbt macros.

    Returns:
        A list of directories in the macros path.
    """
    list_of_sources = [directory for directory in os.listdir(macros_path) if
                       path.isdir(path.join(macros_path, directory))]
    list_of_sources.sort()
    return list_of_sources


def get_sources_wanted(list_of_sources):
    """Get a list of sources wanted for the dbt project.

    Prompts the user to enter a comma-separated list of sources
    they want to use in their dbt project. The function
    will remove any duplicates and return a list of the selected sources.

    Args:
        list_of_sources: A list of available sources.

    Returns:
        A list of selected sources.
    """
    print("\nWhich set of standard models would you like to use in this dbt project? Here's the list:")
    for source in list_of_sources:
        print(source)
    print("\nYou can just add one or input a comma separated list (no brackets or quotes necessary): ")
    sources_wanted = input()
    print()
    sources_wanted = list({source.strip()
                          for source in sources_wanted.split(',')})
    return sources_wanted


def overwrite_choice(source_file_path, output, destination_file_path):
    """Determine whether to overwrite a destination file with a source file.

    Compares the contents of the source file and the destination file,
    and prompts the user to decide whether to
    overwrite the destination file with the source file.
    If the files are identical, the function returns False without
    prompting the user.

    Args:
        source_file_path: The path to the source file.
        output: The contents of the source file as a list of strings.
        destination_file_path: The path to the destination file.

    Returns:
        A boolean indicating whether to overwrite the destination file (True)
        or keep the destination file (False).
    """

    with open(destination_file_path, 'r', encoding='utf-8') as d:
        d_text = list(filter(lambda x: not x.startswith(
            '-- depends_on:'), d.readlines()))
        differences = list(difflib.unified_diff(
            d_text, output, fromfile=destination_file_path,
            tofile=source_file_path, lineterm='\n'))
        if not differences:
            print(
                f"No differences between {destination_file_path} and {source_file_path}, skipping.")
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
    """Extract dependencies from a destination file.

    Args:
        output: The list of strings representing the output file.
        destination_file_path: The path to the destination file.
        dependencies_regex: The regular expression to use for extracting dependencies.

    Returns:
        A list of dependencies extracted from the destination file.
    """
    with open(destination_file_path, 'r', encoding='utf-8') as d:
        dependencies = dependencies_regex.findall(d.read())
    if not dependencies:
        return output
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
    """Determine whether to write a new file from a source file.

    Prompts the user to decide whether to write a new file at the specified
        destination path using the contents of the
        source file.

    Args:
        output: The contents of the source file as a list of strings.
        destination_file_path: The path to the destination file.

    Returns:
        A boolean indicating whether to write a new file at the destination path (True)
        or skip writing the file (False).
    """
    print(f"\nYou do not have a model at {destination_file_path}\n")
    print("Here is the standard model in arc-dbt-functions:\n")
    print(*output, sep='')
    print(
        f"Would you like to create a model with this content at {destination_file_path}?\n")
    yes = None
    while yes not in ('y', 'n'):
        print("Enter y for (y)es or n for (n)o:")
        yes = input()
    return yes == 'y'


def write_to_file(
        file_path,
        destination_path,
        file,
        source,
        model_type,
        create):
    """Write a list of strings to a file at the specified destination path.

    Args:
        destination_file_path: The path to the destination file.
    """
    rx = re.compile(r"\{%\s+macro\s+(.*?)\s+%\}", re.DOTALL)
    dependencies_regex = re.compile(r"--\s+depends_on:\s+.*\n")
    git_prepend = "https://github.com/bsd/dbt-arc-functions/blob/main/macros"
    destination_file_path = path.join(destination_path, file)
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        if file.endswith('.sql'):
            function = rx.search(content)[1] if rx.search(content) else None
            github_path = '/'.join([git_prepend, source, model_type, file])
            output = DBT_STRING.format(github_path=github_path,
                                       function=function)
        else:
            output = content
        if path.exists(destination_file_path) and not create:
            output_formatted = [line + ('\n' if i < len(output.split('\n')) else '')
                                for i, line in enumerate(output.split('\n'))]
            if output_formatted[-1].strip() == '':
                output_formatted = output_formatted[:-1]
            if not overwrite_choice(
                    file_path,
                    output_formatted,
                    destination_file_path):
                return
            output = extract_dependencies(
                output, destination_file_path, dependencies_regex)
        if (
            not path.exists(destination_file_path)
            and not create
            and not write_new_file_choice(output, destination_file_path)
        ):
            return
        if output:
            with open(destination_file_path, 'w', encoding='utf-8') as tf:
                tf.writelines(output)
                print(f"{destination_file_path} created successfully!")


def create_or_update_docs(docs_path, destination_path):
    """Create or update documentation for standard models.

    Processes each model file in the specified documentation path,
        determining whether to overwrite or create new files
    at the destination path. If a file already exists at the destination path,
        it will be overwritten if there are
    differences between the source file and the destination file. If a file does not exist
        at the destination path, a new file will be created.

    Args:
        docs_path: The path to the documentation directory.
        destination_path: The path to the destination directory for the processed models.
    """
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    schema_path = path.join(destination_path, 'schema.yml')
    if not path.exists(schema_path):
        schema_dict = {'version': 2, 'models': []}
    else:
        with open(schema_path, 'r', encoding='utf-8') as f:
            content = f.read()
            schema_dict = yaml.load(content)
    with open(docs_path, 'r', encoding='utf-8') as f:
        content = f.read()
        docs_dict = yaml.load(content)
    docs_model = docs_dict['models'][0]
    model_name = docs_model['name']
    for i, model in enumerate(schema_dict['models']):
        if model['name'] == model_name:
            existing_model_index = i
            break
    else:
        schema_dict['models'].append(docs_model)
        print(
            f"\nCan we add docs for the current model to your schema.yml in {destination_path}?")
        print("This is the data we'd add to your schema.yml:")
        print(docs_model)
        print("The resultant yaml would look like this:")
        yaml.dump(schema_dict, sys.stdout)
        choice = None
        while choice not in ['y', 'n']:
            choice = input("Type y for (y)es or n for (n)o:\n")
        if choice == 'y':
            with open(schema_path, 'w', encoding='utf-8') as f:
                yaml.dump(schema_dict, f)
        return
    if docs_model != schema_dict['models'][existing_model_index]:
        docs_model_set = set(docs_model)
        schema_dict_model_set = set(
            schema_dict['models'][existing_model_index])
        print(
            "Your existing in the schema file for this model"
            "\nis different than the one in our documentation."
            "\nHere's what exists in our documentation but not your schema file:")
        print(schema_dict_model_set - docs_model_set)
        print("Here's what exists in your schema file but not our documentation:")
        print(docs_model_set - schema_dict_model_set)
        print("Would you like to update your schema file to match our documentation?")
        choice = None
        while choice not in ['y', 'n']:
            choice = input("Type y for (y)es or n for (n)o:\n")
        if choice == 'y':
            schema_dict['models'][existing_model_index] = docs_model
            with open(schema_path, 'w', encoding='utf-8') as f:
                yaml.dump(schema_dict, f)
    return


def delete_non_standard_model_choice(destination_file_path):
    # sourcery skip: use-file-iterator
    """
    This gets the user's choice to delete a file not found in standard models folder
    :param destination_file_path: file path to model not found in standard models
    :return: None
    """
    print('---------')
    with open(destination_file_path, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            print(line)
    print('---------')
    print(f"Model exists at {destination_file_path}")
    print("File contents printed above. This file does not exist in the standard models.")
    print("Should I delete this model?")
    yes = None
    while yes not in ('y', 'n'):
        print("Enter y for (y)es or n for (n)o:")
        yes = input()
    if yes == 'y':
        os.remove(destination_file_path)


def process_sources(
        sources_wanted,
        list_of_sources,
        macros_path,
        create,
        destination):
    """
    :param sources_wanted: list of standard models that a client wants to copy to their dbt repo
    :param list_of_sources: list of available standard models within dbt-arc-functions
    :param macros_path: path of the macros folders
    :param create: whether a client wants to create standard models (false if they want to update)
    :param destination: destination dbt repo where client
        would like to create/update standard models
    :return: None
    """
    # TODO this function is very large and therefore hard to parse, break into
    # smaller chunks
    sources_path = path.join('..', 'sources')
    model_types = [sources_path, 'staging', 'marts']
    for source in sources_wanted:
        if source not in list_of_sources:
            print(f'Sorry, {source} is not in the list of sources above')
            continue
        list_of_models = os.listdir(path.join(macros_path, source))
        list_of_models.append(sources_path)
        for model_type in list_of_models:
            if model_type not in model_types:
                print(
                    f"Weird, {model_type} is not one of our standard model types. Going to skip.")
                continue
            source_path = sources_path if model_type == sources_path else path.join(
                macros_path, source, model_type)
            destination_path = path.join(
                destination,
                model_type,
                source) if model_type != sources_path else path.join(
                destination,
                'sources')
            if path.exists(
                    destination_path) and create and not destination_path.endswith('sources'):
                print(
                    f"\nThis directory: {destination_path} already exists and I don't want to overwrite anything in create mode."
                )
                continue
            if not path.exists(destination_path) and not create:
                print(
                    f"\nThis directory: {destination_path} doesn't exist and I don't want to make directories in update mode."
                )
                continue
            elif create and not path.exists(destination_path):
                os.makedirs(destination_path)
            for _, _, files in os.walk(source_path):
                for file in files:
                    if file.endswith('.sql') or file == f"{source}.yml":
                        source_file_path = path.join(source_path, file)
                        write_to_file(source_file_path, destination_path,
                                      file, source, model_type, create)
                        if file.endswith('sql'):
                            docs_path = source_path.replace(
                                'macros', 'documentation')
                            docs_file = file.replace('sql', 'yml')
                            docs_file_path = path.join(docs_path, docs_file)
                            if os.path.exists(docs_file_path):
                                create_or_update_docs(
                                    docs_file_path, destination_path)
                            else:
                                print(
                                    f"\nNo documentation exists for this model, consider adding some at {docs_file_path}!\n")
            for _, _, files in os.walk(destination_path):
                for file in files:
                    destination_file_path = path.join(destination_path, file)
                    source_file_path = path.join(source_path, file)
                    docs_path = source_path.replace('macros', 'documentation')
                    docs_file = file.replace('sql', 'yml')
                    docs_file_path = path.join(docs_path, docs_file)
                    if (not os.path.exists(source_file_path)) and (
                            not os.path.exists(docs_file_path) and (file != 'schema.yml')):
                        delete_non_standard_model_choice(destination_file_path)


def create_or_update_readme(dbt_models_path, list_of_sources):
    dbt_path_parts = Path(dbt_models_path).parts
    name_of_project = dbt_path_parts[-2]
    dbt_base_path_parts = dbt_path_parts[:-1]
    dbt_base_path = os.path.join(*dbt_base_path_parts)
    readme_path = os.path.join(dbt_base_path, 'README.md')
    proposed_readme_string = ""
    proposed_readme_string += f'# {name_of_project}\n\n'
    proposed_readme_string += '## Standard Models from dbt-arc-functions\n\n'
    set_of_existing_models = set()
    marts_path = os.path.join(dbt_models_path, 'marts')
    staging_path = os.path.join(dbt_models_path, 'staging')
    set_of_existing_models.update(os.listdir(marts_path))
    set_of_existing_models.update(os.listdir(staging_path))
    list_of_existing_models = list(set_of_existing_models)
    list_of_existing_models.sort
    for model in list_of_existing_models:
        if model in list_of_sources:
            proposed_readme_string += f"- {model}\n"
    with open(readme_path, "r") as f:
        current_readme_string = f.read()
    differences = list(difflib.unified_diff(
        current_readme_string.split('\n'),
        proposed_readme_string.split('\n'),
        fromfile='Your current readme',
        tofile='Our proposed readme', lineterm='\n'))
    if not differences:
        return
    print("We'd like to propose a new README for your project.\n")
    print("*****Here is your current README:*****\n")
    print(current_readme_string)
    print("*****Here is our proposed README:*****\n")
    print(proposed_readme_string)
    print("*****Here are the differences between the two files:*****\n")
    print(*differences, sep='\n')
    print('\n\n*****Would you like to overwrite your current README?*****')
    overwrite_choice = input(
        "Only an answer of 'yes' will overwrite your README:\n")
    if overwrite_choice == 'yes':
        with open(readme_path, 'w') as f:
            f.write(proposed_readme_string)


def main(dbt_models_path=''):
    """Create or update standard models in a dbt project.

    Prompts the user to choose whether to create new standard models or
        update existing standard models in a dbt project.
    The user is also prompted to enter the absolute or relative path
        where the models should be created or updated.
    The user is then prompted to select the sources they want to use for
        the dbt project. Finally, the selected sources  are processed,
        creating or updating the standard models at the specified path.

    Args:
        dbt_models_path: The path to the dbt models directory.
            If not provided, the user will be prompted to enter the path.
    """
    macros_path = path.join('..', 'macros')
    create = get_create_or_update()
    try:
        if not dbt_models_path:
            dbt_models_path = get_destination()
        list_of_sources = get_list_of_sources(macros_path)
        sources_wanted = get_sources_wanted(list_of_sources)
        process_sources(sources_wanted, list_of_sources,
                        macros_path, create, dbt_models_path)
    except Exception as exception_noted:
        print(exception_noted)
    create_or_update_readme(dbt_models_path, list_of_sources)
    print('\nProgram terminated successfully!\n')


if __name__ == '__main__':
    main()
