import os
import re

ref_pattern = re.compile(r"\{\{\s*ref\(\s*('\w+')\s*\)\s*\}\}")
source_pattern = re.compile(r"\{\{\s*source\(\s*('\w+')\s*,\s*('\w+')\s*\)\s*\}\}")


def mk_macros_dir(directory):
    if not os.path.exists(os.path.join(directory, 'macros')):
        os.mkdir(os.path.join(directory, 'macros'))


def loop_through_files_in_dir(directory):
#TODO fix low code quality in this function
    for file_name in os.listdir(directory):
        if file_name.endswith('.sql'):
            with open(os.path.join(directory, file_name), 'r') as f:
                text = f.read()
                text = re.sub('-- depends_on:.*\n', '', text)
                macro_name = file_name.split('.sql')[0]
                if not text.strip().startswith('{% macro'):
                    output = f'{{% macro create_{macro_name}('
                    references = ref_pattern.findall(text)
                    if len(references) > 0:
                        if len(references) == 1:
                            output += f'\n    reference_name={references[0]},'
                            text = ref_pattern.sub(f'{{{{ ref(reference_name) }}}}', text, 1)
                        else:
                            for i, reference_name in enumerate(references):
                                argument_name = f'reference_{i}_name'
                                output += f'\n    {argument_name}={reference_name},'
                                text = ref_pattern.sub(f'{{{{ ref({argument_name}) }}}}', text, 1)
                    sources = source_pattern.findall(text)
                    if len(sources) > 0:
                        if len(sources) == 1:
                            output += f'\n    source_name={sources[0][0]},'
                            output += f'\n    source_table_name={sources[0][1]},'
                            text = source_pattern.sub(f'{{{{ source(source_name,source_table_name) }}}}', text, 1)
                        else:
                            for i, (source_name, source_table_name) in enumerate(sources):
                                source_argument_name = f'source_{i}_name'
                                source_table_argument_name = f'source_{i}_table_name'
                                output += f'\n    {source_argument_name}={source_name},'
                                output += f'\n    {source_table_argument_name}={source_table_name},'
                                text = source_pattern.sub(
                                    f'{{{{ source({source_argument_name},{source_table_argument_name}) }}}}', text, 1)
                    output = output[:-1] if output.endswith(',') else output
                    output += f') %}}\n'
                    output += text
                    output += f'\n{{% endmacro %}}'
            with open(os.path.join(directory, 'macros', file_name), 'w') as f:
                f.write(output)

def main():
    directory = input(
        'Please enter either the relative or absolute path of the directory that you want to convert to macros:\n')
    mk_macros_dir(directory)
    loop_through_files_in_dir(directory)

if __name__ == '__main__':
    main()
