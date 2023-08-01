#!/usr/bin/python3

import sys
import os
import yaml


def check_for_no_columns(file_path, docs_without_columns, doc_yaml):
    """Checks if a doc has no columns"""
    try:
        columns = doc_yaml["models"][0]["columns"]
        if not columns or len(columns) < 1:
            docs_without_columns.append(file_path)
    except KeyError:
        docs_without_columns.append(file_path)


def check_table_for_no_columns(
    file_path, tables_without_columns, columns_without_info, table, doc_yaml
):
    try:
        columns = table["columns"]
        if not columns or len(columns) < 1:
            tables_without_columns.append((file_path, table["name"]))
    except KeyError:
        tables_without_columns.append(
            (file_path, table["name"], doc_yaml["sources"][0]["schema"])
        )
        return
    except TypeError:
        tables_without_columns.append((file_path, ""))
        return
    if columns:
        for column in columns:
            try:
                column["description"]
                column["data_type"]
            except KeyError:
                columns_without_info.append(
                    (
                        file_path,
                        table["name"],
                        column["name"],
                        doc_yaml["sources"][0]["schema"],
                    )
                )


def check_for_no_tables_or_tables_no_columns(
    file_path,
    sources_without_tables,
    tables_without_columns,
    columns_without_info,
    var_sources_without_extra,
    doc_yaml,
):
    try:
        tables = doc_yaml["sources"][0]["tables"]
        if not tables or len(tables) < 1:
            sources_without_tables.append(file_path)
    except KeyError:
        sources_without_tables.append(file_path)
        return
    if isinstance(tables, str):
        original_filepath = file_path
        file_path = file_path.replace("sources", "sources-extra-for-fake-data")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                doc_yaml = yaml.safe_load(f)
        except FileNotFoundError:
            var_sources_without_extra.append([original_filepath, file_path])
            return
    else:
        for table in tables:
            check_table_for_no_columns(
                file_path, tables_without_columns, columns_without_info, table, doc_yaml
            )


def check_for_no_version(file_path, docs_without_version, doc_yaml):
    """Checks if a doc has no version number"""
    try:
        doc_yaml["version"]
    except KeyError:
        docs_without_version.append(file_path)


def check_for_no_macro(file_path, docs_without_macro, doc_yaml):
    """Checks if a doc has no macro"""
    macro_path = file_path.replace("documentation", "macros").replace(".yml", ".sql")
    if not os.path.exists(macro_path):
        docs_without_macro.append((file_path, macro_path))


def check_for_blank_doc(file_path, docs_without_content, doc_yaml):
    try:
        doc_yaml["models"]
    except TypeError:
        docs_without_content.append(file_path)
        raise TypeError


def check_for_wrong_names(file_path, docs_with_wrong_names, doc_yaml):
    file_stem = os.path.splitext(file_path)[0].split("/")[-1]
    try:
        if doc_yaml["models"][0]["name"] != file_stem:
            docs_with_wrong_names.append(file_path)
    except TypeError:
        docs_with_wrong_names.append(file_path)
        raise TypeError


def get_incorrect_docs():
    docs_without_columns = []
    docs_without_version = []
    docs_without_macro = []
    docs_without_content = []
    docs_with_wrong_names = []

    for root, _, files in os.walk("../documentation"):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".yml") and "utils" not in root:
                with open(file_path, "r", encoding="utf-8") as f:
                    doc_yaml = yaml.safe_load(f)
                try:
                    check_for_blank_doc(file_path, docs_without_content, doc_yaml)
                except TypeError:
                    continue
                check_for_no_columns(file_path, docs_without_columns, doc_yaml)
                check_for_no_version(file_path, docs_without_version, doc_yaml)
                check_for_no_macro(file_path, docs_without_macro, doc_yaml)
                check_for_wrong_names(file_path, docs_with_wrong_names, doc_yaml)

    return (
        docs_without_columns,
        docs_without_version,
        docs_without_macro,
        docs_without_content,
        docs_with_wrong_names,
    )


# TODO add print statement for columns without info
def get_incorrect_sources():
    sources_without_tables = []
    tables_without_columns = []
    sources_without_version = []
    columns_without_info = []
    var_sources_without_extra = []

    for root, _, files in os.walk("../sources"):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".yml") and "utils" not in root:
                with open(file_path, "r", encoding="utf-8") as f:
                    source_yaml = yaml.safe_load(f)

                check_for_no_tables_or_tables_no_columns(
                    file_path,
                    sources_without_tables,
                    tables_without_columns,
                    columns_without_info,
                    var_sources_without_extra,
                    source_yaml,
                )
                check_for_no_version(file_path, sources_without_version, source_yaml)

    return (
        sources_without_tables,
        tables_without_columns,
        sources_without_version,
        columns_without_info,
        var_sources_without_extra,
    )


def print_missing_info(list_of_missing_info, format_string, entity_string):
    for missing_info in list_of_missing_info:
        print(format_string.format(missing_info=missing_info))
    print(f"{entity_string}: {len(list_of_missing_info)}")


DOCS_WITHOUT_MACRO_FORMAT_STRING = """
The doc below doesn't have a macro associated with it:
 {missing_info[0]}
Expected to find doc here:
 {missing_info[1]}
Please delete the doc, then run create_docs.ipynb against a working
client to create docs
"""


DOCS_WITHOUT_COLUMNS_FORMAT_STRING = """
The doc below doesn't have any columns:
{missing_info}
Please delete the doc, then run create_docs.ipynb against a working
client to create docs
"""


DOCS_WITHOUT_VERSION_FORMAT_STRING = """
The doc below doesn't have a version number:
 {missing_info}
Please delete the doc, then run create_docs.ipynb against a working client to create docs.
"""

DOCS_WITH_WRONG_NAMES_FORMAT_STRING = """
The doc below has the wrong name:
 {missing_info}
The name should be the same as the file name without the yml extension.
Please correct manually or delete the doc, then run create_docs.ipynb against a working
client to create docs.
"""


SOURCES_WITHOUT_TABLES_FORMAT_STRING = """
The source below doesn't have any tables:
 {missing_info}

PLEASE NOTE:
For the command below to work, your dbt-codegen will need to be on the main branch as of 5/8/2023
Add this to your packages.yml:
  - git: https://github.com/dbt-labs/dbt-codegen.git
    revision: main

Please run the following command against a working and reformat your source:
 dbt run-operation generate_source --args '{{"schema_name": "SCHEMA", "table_names":["TABLE"],\
      "generate_columns": "true", "include_data_types": "true", "include_descriptions": "true"}}'
"""


DOCS_WITHOUT_CONTENT_FORMAT_STRING = """
The doc below doesn't have content:
 {missing_info}
Please delete the doc, then run create_docs.ipynb against a working client to create docs.
"""

TABLES_WITHOUT_COLUMNS_FORMAT_STRING = """
The table noted in the source below doesn't have any columns:"
    {missing_info[0]}
    {missing_info[1]}

PLEASE NOTE:
For the command below to work, your dbt-codegen will need to be on the main branch as of 5/8/2023
Add this to your packages.yml:
  - git: https://github.com/dbt-labs/dbt-codegen.git
    revision: main

Please run the following command against a working and reformat your source:
 dbt run-operation generate_source --args '{{"schema_name": "{missing_info[2]}", \
     "table_names":["{missing_info[1]}"], "generate_columns": "true", \
     "include_data_types": "true", "include_descriptions": "true"}}'"""


SOURCES_WITHOUT_VERSION_FORMAT_STRING = """
The source below doesn't have a version number:
 {missing_info}
Please delete the source, then run create_sources.ipynb against a working client to create sources
"""

COLUMNS_WITHOUT_INFO_FORMAT_STRING = """
The column in the table in the source below does not have info:
 {missing_info[0]}
 {missing_info[1]}
 {missing_info[2]}

PLEASE NOTE:
For the command below to work, your dbt-codegen will need to be on the main branch as of 5/8/2023
Add this to your packages.yml:
  - git: https://github.com/dbt-labs/dbt-codegen.git
    revision: main

Please run the following command against a working and reformat your source:
 dbt run-operation generate_source --args '{{"schema_name": "{missing_info[3]}", "table_names":["{missing_info[1]}"],\
      "generate_columns": "true", "include_data_types": "true", "include_descriptions": "true"}}'
"""

VAR_SOURCES_WITHOUT_EXTRA_FORMAT_STRING = """
The source below should have a document in sources-extra-for-fake-data:
 {missing_info[0]}
This is because it has a string in place of a list of tables in its source file.
We need a real source file with a list of tables to be able to build fake data for the arc-standard-dashboard.
Please create a new source file in this location:
 {missing_info[1]}

PLEASE NOTE:
For the command below to work, your dbt-codegen will need to be on the main branch as of 5/8/2023
Add this to your packages.yml:
  - git: https://github.com/dbt-labs/dbt-codegen.git
    revision: main

Please run the following command, replacing SCHEMA_NAME and TABLE_NAME appropriately,
 against a working and add the result to {missing_info[1]}:
  dbt run-operation generate_source --args '{{"schema_name": "SCHEMA_NAME", "table_names":["TABLE_NAME"],\
       "generate_columns": "true", "include_data_types": "true", "include_descriptions": "true"}}'

"""


def main():
    (
        docs_without_columns,
        docs_without_version,
        docs_without_macro,
        docs_without_content,
        docs_with_wrong_names,
    ) = get_incorrect_docs()
    (
        sources_without_tables,
        tables_without_columns,
        sources_without_version,
        columns_without_info,
        var_sources_without_extra,
    ) = get_incorrect_sources()

    print_missing_info(
        docs_without_content, DOCS_WITHOUT_CONTENT_FORMAT_STRING, "Docs without content"
    )
    print_missing_info(
        docs_without_macro, DOCS_WITHOUT_MACRO_FORMAT_STRING, "Docs without macros"
    )
    print_missing_info(
        docs_without_columns, DOCS_WITHOUT_COLUMNS_FORMAT_STRING, "Docs without columns"
    )
    print_missing_info(
        docs_without_version, DOCS_WITHOUT_VERSION_FORMAT_STRING, "Docs without version"
    )
    print_missing_info(
        docs_with_wrong_names,
        DOCS_WITH_WRONG_NAMES_FORMAT_STRING,
        "Docs with wrong names",
    )
    print_missing_info(
        sources_without_tables,
        SOURCES_WITHOUT_TABLES_FORMAT_STRING,
        "Sources without tables",
    )
    print_missing_info(
        tables_without_columns,
        TABLES_WITHOUT_COLUMNS_FORMAT_STRING,
        "Tables without columns",
    )
    print_missing_info(
        sources_without_version,
        SOURCES_WITHOUT_VERSION_FORMAT_STRING,
        "Sources without version",
    )
    print_missing_info(
        columns_without_info,
        COLUMNS_WITHOUT_INFO_FORMAT_STRING,
        "Source columns without info",
    )
    print_missing_info(
        var_sources_without_extra,
        VAR_SOURCES_WITHOUT_EXTRA_FORMAT_STRING,
        "var(sources) sources without files in the sources-extra-for-fake-data directory",
    )

    if (
        docs_without_content
        or docs_without_columns
        or docs_without_version
        or docs_without_macro
        or docs_with_wrong_names
        or sources_without_tables
        or sources_without_version
        or columns_without_info
        or var_sources_without_extra
    ):
        sys.exit(1)


if __name__ == "__main__":
    main()
