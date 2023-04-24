#!/usr/bin/python3

import sys
import os
import yaml


def main():
    docs_without_columns = []

    for root, _, files in os.walk('../documentation'):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.yml'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    doc_yaml = yaml.safe_load(f)
                try:
                    columns = doc_yaml['models'][0]['columns']
                except KeyError:
                    print('')
                    docs_without_columns.append(file_path)
                if not columns or len(columns) < 2:
                    docs_without_columns.append(file_path)


    if docs_without_columns:
        for doc_without_columns in docs_without_columns:
            print(f"The doc below doesn't have any columns:\n {doc_without_columns}\n"
                  "Please delete the doc, then run create_docs.ipynb against a working"
                  " client to create docs\n")

        print(f"\nDocs without columns: {len(docs_without_columns)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
