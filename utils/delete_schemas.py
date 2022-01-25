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

def main(credentials_path='',username=''):
    if not credentials_path:
        credentials_path = input("\nPlease enter the path to your BigQuery credentials JSON:\n")
    if not username:
        username = input("\nPlease enter your dbt username OR your bluestate email:\n")
        if '@bluestate.co' in username:
            username = username.split('@')[0]
            username = 'dbt_'+username
    print(function_helptext)
    client = get_client(credentials_path)
    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project
    if datasets:
        print(f"\nSchemas in project {project} with prepend {username}:")
        for dataset in datasets:
            if dataset.dataset_id.startswith(username):
                print(f"\t{dataset.dataset_id} exists in your database")
                delete_choice = input('Enter yes to delete the schema (no other input but yes will delete the schema):\n')
                if delete_choice == 'yes':
                    client.delete_dataset(
                        dataset.dataset_id, delete_contents=True, not_found_ok=True
                    )

if __name__ == '__main__':
    main()
