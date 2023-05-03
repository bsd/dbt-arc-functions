#!/usr/bin/python3

import sys
import os
import yaml


def check_for_no_columns(file_path, docs_without_columns, doc_yaml):
    try:
        columns = doc_yaml['models'][0]['columns']
        if not columns or len(columns) < 2:
            docs_without_columns.append(file_path)
    except KeyError:
        docs_without_columns.append(file_path)


def check_for_no_tables_or_tables_no_columns(
        file_path,
        sources_without_tables,
        tables_without_columns,
        columns_without_info,
        doc_yaml):
    try:
        tables = doc_yaml['sources'][0]['tables']
        if not tables or len(tables) < 1:
            sources_without_tables.append(file_path)
    except KeyError:
        sources_without_tables.append(file_path)
        return
    if isinstance(tables, str):
        return
    else:
        for table in tables:
            try:
                columns = table['columns']
                if not columns or len(columns) < 2:
                    tables_without_columns.append((file_path, table['name']))
            except KeyError:
                tables_without_columns.append((file_path, table['name']))
                return
            except TypeError:
                tables_without_columns.append((file_path, ''))
                return
            if columns:
                for column in columns:
                    try:
                        description = column['description']
                        data_type = column['data_type']
                    except KeyError:
                        columns_without_info.append((file_path, table['name'], column['name']))


def check_for_no_version(file_path, docs_without_version, doc_yaml):
    try:
        doc_yaml['version']
    except KeyError:
        docs_without_version.append(file_path)


def check_for_no_macro(file_path, docs_without_macro, doc_yaml):
    macro_path = file_path.replace(
        'documentation',
        'macros').replace(
        '.yml',
        '.sql')
    if not os.path.exists(macro_path):
        docs_without_macro.append((file_path, macro_path))


def get_incorrect_docs():
    docs_without_columns = []
    docs_without_version = []
    docs_without_macro = []

    for root, _, files in os.walk('../documentation'):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.yml') and 'utils' not in root:
                with open(file_path, 'r', encoding='utf-8') as f:
                    doc_yaml = yaml.safe_load(f)
                check_for_no_columns(file_path, docs_without_columns, doc_yaml)
                check_for_no_version(file_path, docs_without_version, doc_yaml)
                check_for_no_macro(file_path, docs_without_macro, doc_yaml)

    return (docs_without_columns, docs_without_version, docs_without_macro)


# TODO add print statement for columns without info
def get_incorrect_sources():
    sources_without_tables = []
    tables_without_columns = []
    sources_without_version = []
    columns_without_info = []

    for root, _, files in os.walk('../sources'):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.yml') and 'utils' not in root:
                with open(file_path, 'r', encoding='utf-8') as f:
                    source_yaml = yaml.safe_load(f)

                check_for_no_tables_or_tables_no_columns(
                    file_path,
                    sources_without_tables,
                    tables_without_columns,
                    columns_without_info,
                    source_yaml)
                check_for_no_version(
                    file_path, sources_without_version, source_yaml)
                

    return (sources_without_tables, tables_without_columns, sources_without_version)


def print_missing_info(list_of_missing_info, format_string):
    for missing_info in list_of_missing_info:
        print(format_string.format(missing_info=missing_info))


def main():
    (docs_without_columns,
     docs_without_version,
     docs_without_macro) = get_incorrect_docs()
    (sources_without_tables,
     tables_without_columns,
     sources_without_version) = get_incorrect_sources()
    docs_without_macro_format_string="""The doc below doesn't have a macro associated with it:
 {missing_info[0]}
Expected to find doc here:
 {missing_info[1]}
Please delete the doc, then run create_docs.ipynb against a working
client to create docs"""
    print_missing_info(docs_without_macro, docs_without_macro_format_string)


    for doc_without_macro in docs_without_macro:
        print(
            f"The doc below doesn't have a macro associated with it:\n {doc_without_macro[0]}\n"
            f"Expected to find doc here:\n {doc_without_macro[1]}\n"
            "Please delete the doc, then run create_docs.ipynb against a working"
            " client to create docs\n")
    print(f"\nDocs without macro: {len(docs_without_macro)}")
    for doc_without_columns in docs_without_columns:
        print(
            f"The doc below doesn't have any columns:\n {doc_without_columns}\n"
            "Please delete the doc, then run create_docs.ipynb against a working"
            " client to create docs\n")
    print(f"\nDocs without columns: {len(docs_without_columns)}")
    for doc_without_version in docs_without_version:
        print(
            f"The doc below doesn't have a version number:\n {doc_without_version}\n"
            "Please delete the doc, then run create_docs.ipynb against a working"
            " client to create docs\n")
    print(f"\nDocs without version: {len(docs_without_version)}")
    for source_without_tables in sources_without_tables:
        print(
            f"The source below doesn't have any tables:\n {source_without_tables}\n"
            "Please run the following command against a working and reformat your source:\n"
            """ dbt run-operation generate_source --args '{"schema_name": "SCHEMA", """
            """"table_names":["TABLE"], "generate_columns": "true", "include_data_types": "true",}'\n""")
    print(f"\nsources without tables: {len(sources_without_tables)}")
    for table_without_columns in tables_without_columns:
        print(
            "The table noted in the source below doesn't have any columns:\n"
            f" {table_without_columns[0]}\n{table_without_columns[1]}\n"
            "Please run the following command against a working and reformat your source:\n"
            """ dbt run-operation generate_source --args '{"schema_name": "SCHEMA", """
            """"table_names":["TABLE"], "generate_columns": "true", "include_data_types": "true",}'\n""")
    print(f"\nsource tables without columns: {len(tables_without_columns)}")
    for source_without_version in sources_without_version:
        print(
            f"The source below doesn't have a version number:\n {source_without_version}\n"
            "Please delete the source, then run create_sources.ipynb against a working"
            " client to create sources\n")
    print(f"\nsources without version: {len(sources_without_version)}")

    if (docs_without_columns
        or docs_without_version
        or docs_without_macro
        or sources_without_tables
            or sources_without_version):
        sys.exit(1)


if __name__ == "__main__":
    main()
