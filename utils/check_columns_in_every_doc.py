#!/usr/bin/python3

import sys
import os
import yaml


def check_for_no_columns(file_path, docs_without_columns, doc_yaml):
    try:
        columns = doc_yaml['models'][0]['columns']
    except KeyError:
        docs_without_columns.append(file_path)
    if not columns or len(columns) < 2:
        docs_without_columns.append(file_path)


def check_for_no_version(file_path, docs_without_version, doc_yaml):
    try:
        doc_yaml['version']
    except KeyError:
        docs_without_version.append(file_path)


def main():
    docs_without_columns = []
    docs_without_version = []

    for root, _, files in os.walk('../documentation'):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.yml'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    doc_yaml = yaml.safe_load(f)
                check_for_no_columns(file_path, docs_without_columns, doc_yaml)
                check_for_no_version(file_path, docs_without_version, doc_yaml)

    if docs_without_columns:
        for doc_without_columns in docs_without_columns:
            print(
                f"The doc below doesn't have any columns:\n {doc_without_columns}\n"
                "Please delete the doc, then run create_docs.ipynb against a working"
                " client to create docs\n")
        for doc_without_version in docs_without_version:
            print(
                f"The doc below doesn't have a version number:\n {doc_without_version}\n"
                "Please delete the doc, then run create_docs.ipynb against a working"
                " client to create docs\n")
        print(f"\nDocs without version: {len(docs_without_version)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
