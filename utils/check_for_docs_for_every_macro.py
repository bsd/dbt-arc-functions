#!/usr/bin/python3

import sys
import os


def main():
    macros_without_docs = []

    for root, _, files in os.walk('../macros'):
        for file in files:
            if file.endswith('.sql') and not root.startswith('../macros/utils'):
                macro_path = os.path.join(root, file)
                doc_folder = root.replace('macros','documentation')
                doc_file = file.replace('.sql','.yml')
                doc_path = os.path.join(doc_folder,doc_file)
                if not os.path.exists(doc_path):
                    macros_without_docs.append((macro_path, doc_path))
    
    if macro_without_docs:
        for macro_without_docs in macros_without_docs:
            macro_path, doc_path = macro_without_docs
            print(f"The macro below does not have docs:\n {macro_path}\n"
                f"Expect docs here:\n {doc_path}\n\n")

        print(f"\nMacros without docs: {len(macros_without_docs)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
