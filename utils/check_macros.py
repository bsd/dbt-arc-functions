#!/usr/bin/python3

import sys
import os


def check_macro_docs(root, file, macros_without_docs):
    macro_path = os.path.join(root, file)
    doc_folder = root.replace("macros", "documentation")
    doc_file = file.replace(".sql", ".yml")
    doc_path = os.path.join(doc_folder, doc_file)
    if not os.path.exists(doc_path):
        macros_without_docs.append((macro_path, doc_path))


def print_macros_without_docs(macros_without_docs):
    if macros_without_docs:
        for macro_without_docs in macros_without_docs:
            macro_path, doc_path = macro_without_docs
            print(
                f"The macro below does not have docs:\n {macro_path}\n"
                f"Expect docs here:\n {doc_path}\n\n"
            )

        print(f"\nMacros without docs: {len(macros_without_docs)}")


def check_macro_name_and_file_name(root, file, macros_with_wrong_name):
    macro_path = os.path.join(root, file)
    macro_name = f"create_{file.replace('.sql', '')}"
    with open(macro_path, "r") as f:
        macro_text = f.read()
        if macro_name not in macro_text:
            macros_with_wrong_name.append((macro_path, macro_name))


def print_macros_with_wrong_name(macros_with_wrong_name):
    if macros_with_wrong_name:
        for macro_with_wrong_name in macros_with_wrong_name:
            macro_path, macro_name = macro_with_wrong_name
            print(
                f"The macro in the file below does not have correct name:\n {macro_path}\n"
                f"Expected name: {macro_name}\n\n"
            )

        print(f"\nMacros with wrong name: {len(macros_with_wrong_name)}")


def main():
    macros_without_docs = []
    macros_with_wrong_name = []
    for root, _, files in os.walk("../macros"):
        for file in files:
            if file.endswith(".sql") and not root.startswith("../macros/utils"):
                check_macro_docs(root, file, macros_without_docs)
                check_macro_name_and_file_name(root, file, macros_with_wrong_name)

    print_macros_without_docs(macros_without_docs)
    print_macros_with_wrong_name(macros_with_wrong_name)
    if macros_without_docs or macros_with_wrong_name:
        sys.exit(1)


if __name__ == "__main__":
    main()
