""" This is a python script that creates jinja macros from SQL. """

import os
import re

# Compile regular expressions for matching reference patterns
ref_pattern = re.compile(r"\{\{\s*ref\(\s*('\w+')\s*\)\s*\}\}")

# Compile regular expression for matching source patterns
source_pattern = re.compile(r"\{\{\s*source\(\s*('\w+')\s*,\s*('\w+')\s*\)\s*\}\}")


def mk_macros_dir(directory):
    """Creates a directory named 'macros' in the specified directory if it doesn't exist.
    Args:
        directory (str): the path to the directory where the 'macros' directory will be created 
    """
    # Create the 'macros' directory if it doesn't exist
    if not os.path.exists(os.path.join(directory, 'macros')):
        os.mkdir(os.path.join(directory, 'macros'))

def loop_through_files_in_dir(directory):
    """
    Loops through all the files in the specified directory, 
        and processes only the ones with '.sql' extension.
    For each file, reads its content, removes 'depends_on' lines, 
        and creates a jinja macro with the file name as the macro name.
    The macro takes arguments that correspond to any references or sources in the SQL code, 
        which are replaced with jinja macro calls.
    The resulting jinja macro is written to a file in the 'macros' directory 
        with the same name as the SQL file.
    Args:
        directory (str): the path to the directory containing the SQL files.
    """
# Loop through all the files in the given directory
    for file_name in os.listdir(directory):
        # Process only .sql files
        if file_name.endswith('.sql'):
            with open(os.path.join(directory, file_name), 'r', encoding='utf-8') as f:
                # Read the content of the file
                text = f.read()
                # Remove the 'depends_on' lines
                text = re.sub('-- depends_on:.*\n', '', text)
                # Get the name of the macro
                macro_name = file_name.split('.sql')[0]
                # Check if the text starts with a macro
                if not text.strip().startswith('{% macro'):
                    # Start building the macro output
                    output = f'{{% macro create_{macro_name}('
                    # Find all references in the text
                    references = ref_pattern.findall(text)
                    if len(references) > 0:
                        # Handle the case where there's only one reference
                        if len(references) == 1:
                            output += f'\n    reference_name={references[0]},'
                            # Replace the reference in the text
                            text = ref_pattern.sub(f'{{{{ ref(reference_name) }}}}', text, 1)
                        # Handle the case where there are multiple references
                        else:
                            for i, reference_name in enumerate(references):
                                argument_name = f'reference_{i}_name'
                                output += f'\n    {argument_name}={reference_name},'
                                # Replace each reference in the text
                                text = ref_pattern.sub(f'{{{{ ref({argument_name}) }}}}', text, 1)
                    # Find all sources in the text
                    sources = source_pattern.findall(text)
                    if len(sources) > 0:
                        # Handle the case where there's only one source
                        if len(sources) == 1:
                            output += f'\n    source_name={sources[0][0]},'
                            output += f'\n    source_table_name={sources[0][1]},'
                            # Replace the source in the text
                            text = source_pattern.sub(f'{{{{ source(source_name,source_table_name) }}}}', text, 1)
                        # Handle the case where there are multiple sources
                        else:
                            # Loop through the sources
                            for i, (source_name, source_table_name) in enumerate(sources):
                                # Generate source argument name
                                source_argument_name = f'source_{i}_name'
                                # Generate source table argument name
                                source_table_argument_name = f'source_{i}_table_name'
                                # Add the source argument and its value to the output
                                output += f'\n    {source_argument_name}={source_name},'
                                # Add the source table argument and its value to the output
                                output += f'\n    {source_table_argument_name}={source_table_name},'
                                # Replace the source in the text with the macro call
                                text = source_pattern.sub(
                                    f'{{{{ source({source_argument_name},{source_table_argument_name}) }}}}', text, 1)
                    # Remove the last comma from the output if it exists
                    output = output[:-1] if output.endswith(',') else output
                    # Add the macro closing tag to the output
                    output += f') %}}\n'
                    # Add the text to the output
                    output += text
                    # Add the end macro tag to the output
                    output += f'\n{{% endmacro %}}'
            with open(os.path.join(directory, 'macros', file_name), 'w', encoding='utf-8') as f:
                 # Write the output to the file
                f.write(output)

def main():
    """The main function of the script, which prompts the user to enter 
        the path to the directory containing the SQL files.
    Calls mk_macros_dir() to create the 'macros' directory, 
        and loop_through_files_in_dir() to process the SQL files and create jinja macros.
    """
    directory = input(
        'Please enter either the relative or absolute path of the\
            directory that you want to convert to macros:\n')
    mk_macros_dir(directory)
    loop_through_files_in_dir(directory)

if __name__ == '__main__':
    main()
