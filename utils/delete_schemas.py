#!/usr/bin/env python
# coding: utf-8
from google.cloud import bigquery
from google.oauth2 import service_account


def get_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id, )


function_helptext = """\nSometimes it helps to delete existing personal dbt schemas before doing a 
dbt build to delete outdated tables and views"""

prepend_helptext = """\ndbt user schemas usually start with 'dbt_'.
Can I add dbt_ to your username to check for schemas that start with dbt_{username}?
Enter y for (y)es or n for (n)o:\n
"""


def main(credentials_path='', dbt_username=''):
    if not credentials_path:
        credentials_path = input("\nPlease enter the path to your BigQuery credentials JSON:\n")
    if not dbt_username:
        dbt_username = input("\nPlease enter your dbt username OR your bluestate email:\n")
        if '@bluestate.co' in dbt_username:
            dbt_username = dbt_username.split('@')[0]
    if not dbt_username.startswith('dbt_'):
        prepend_choice = input(prepend_helptext.format(username=dbt_username))
        if prepend_choice == 'y':
            dbt_username = f'dbt_{dbt_username}'
    print(function_helptext)
    client = get_client(credentials_path)
    if datasets := list(client.list_datasets()):
        project = client.project
        print(f"\nSchemas in project {project} with prepend {dbt_username}:")
        for dataset in datasets:
            if dataset.dataset_id.startswith(dbt_username):
                print(f"\t{dataset.dataset_id} exists in your database")
                delete_choice = input(
                    'Enter yes to delete the schema (no other input but yes will delete the schema):\n')
                if delete_choice == 'yes':
                    client.delete_dataset(
                        dataset.dataset_id, delete_contents=True, not_found_ok=True
                    )


if __name__ == '__main__':
    main()
