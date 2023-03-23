""" This module compiles and generates a sources YML in dbt based on a regex match """

# coding: utf-8
# TODO: Make source_regex_mappings into an external json file
# TODO: add vars: ga4_schema to the dbt_project.yml file that pulls up the ga4 schema

import os
import re
import json

from utils import initialize_yaml
from google.cloud import bigquery
from google.oauth2 import service_account

source_regex_mappings = {
    'frakture_twitter_paidmedia.yml': {
        'schema': 'src_frakture',
        'tables': [r'^twitter_[A-Za-z0-9]{3}_message$',
                   r'^twitter_[A-Za-z0-9]{3}_ad_summary_by_date$'
                   ]
    },
    'frakture_google_ads_paidmedia.yml': {
        'schema': 'src_frakture',
        'tables': [r'^google_ads_[A-Za-z0-9]{3}_message$',
                   r'^google_ads_[A-Za-z0-9]{3}_ad_summary_by_date$'
                   ]
    },
    'frakture_facebook_paidmedia.yml': {
        'schema': 'src_frakture',
        'tables': [r'^facebook_bizman_[A-Za-z0-9]{3}_message$',
                   r'^facebook_bizman_[A-Za-z0-9]{3}_ad_summary_by_date$'
                   ]
    },
    'frakture_everyaction_email.yml': {
        'schema': 'src_frakture',
        'tables': [r'^everyaction_[A-Za-z0-9]{3}_email_summary$',
                   r'^everyaction_[A-Za-z0-9]{3}_message$'
                   ]
    },
    'frakture_everyaction_person.yml': {
        'schema': 'src_frakture',
        'tables': [r'^everyaction_[A-Za-z0-9]{3}_per_person_message_stat$',
                   r'^everyaction_[A-Za-z0-9]{3}_person$',
                   ]
    },
    'frakture_sfmc_person.yml': {
        'schema': 'src_frakture',
        'tables': [r'^sfmc_[A-Za-z0-9]{3}_per_person_message_stat$',
                   r'^sfmc_[A-Za-z0-9]{3}_person$',
                   ]
    },
    'ga4_google_analytics_web.yml': {
        'schema': [r'^analytics_%', 
                   ],
### ADD IDENTIFER AND SOURCE YEML
    },

}

CREDENTIALS_HELPTEXT = """
If you'd like to know how to generate a credentials json go here:
https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials

We need to use your Big Query Credentials to access the database and build out sources.

Please enter the absolute location of your credentials json file:
"""

INPLACE_OR_COPY_HELPTEXT = """
Would you like to (r)eplace the existing {filename}.yml or make a (c)opy named {filename}_copy.yml?
Enter r for replace or c for copy:
"""


def load_dbt_project_yml(dbt_project_path, yaml):
    """
    Load the content of the dbt_project.yml file as a dictionary.
    :param dbt_project_path: The file path of the dbt_project.yml file.
    :param yaml: The instance of the ruamel.yaml.YAML class.
    :return: A dictionary containing the content of the dbt_project.yml file.
    """
    with open(dbt_project_path, 'r', encoding='utf-8') as f:
        content = f.read()
        return yaml.load(content)


def set_database(dbt_project_yml, project_id):
    """
    Set the 'database' variable in the dbt_project.yml dictionary.
    :param dbt_project_yml: The dictionary containing the content of the dbt_project.yml file.
    :param project_id: The project ID to set as the 'database' variable.
    """
    variables = {'database': project_id}
    if 'vars' in dbt_project_yml:
        dbt_project_yml['vars']['database'] = project_id
    else:
        dbt_project_yml['vars'] = variables


def get_project_id(dbt_credentials_path):
    """
    Get the project ID from the BigQuery credentials file.
    :param dbt_credentials_path: The file path of the BigQuery credentials file.
    :return: The project ID as a string.
    """
    with open(dbt_credentials_path, 'r', encoding='utf-8') as f:
        return json.load(f)['project_id']


def get_schema(regex_pattern, client):
    """ Get the schema that matches the regex pattern """
    # List all the available datasets in the project
    datasets = list(client.list_datasets())

    # Loop through each dataset and check if it matches the regex pattern
    for dataset in datasets:
        if re.match(regex_pattern, dataset.dataset_id):
            # If the dataset matches the pattern, return its dataset ID
            return dataset.dataset_id

    # If no matching dataset is found, raise an exception
    raise Exception(f"No dataset found matching the regex pattern: {regex_pattern}")


def get_client(credentials_path):
    """
    Create a BigQuery client using the credentials file.
    :param credentials_path: The file path of the BigQuery credentials file.
    :return: A BigQuery client instance.
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"], )
    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )


def add_sources_to_yml(dbt_project_yml, sources_directory, client):
    """
    Add sources to the dbt_project.yml dictionary based on regex matches.
    :param dbt_project_yml: The dictionary containing the content of the dbt_project.yml file.
    :param sources_directory: The directory where the source files are located.
    :param client: The BigQuery client instance.
    :return: The updated dbt_project.yml dictionary.
    """
    if 'sources' not in dbt_project_yml['vars']:
        dbt_project_yml['vars']['sources'] = {}
    for source_yml in os.listdir(sources_directory):
        print(f"Working on {source_yml}")
        if source_yml in source_regex_mappings:
            source, _ = os.path.splitext(source_yml)

            if source not in dbt_project_yml['vars']['sources']:
                dbt_project_yml['vars']['sources'][source] = {}
            if 'tables' not in dbt_project_yml['vars']['sources'][source]:
                dbt_project_yml['vars']['sources'][source]['tables'] = []
            query = f"SELECT table_name FROM `{source_regex_mappings[source_yml]['schema']}.INFORMATION_SCHEMA.TABLES`"
            query_job = client.query(query)
            all_tables = [row[0] for row in query_job]
            for table_regex in source_regex_mappings[source_yml]['tables']:
                r = re.compile(table_regex)
                matching_tables = list(
                    filter(r.match, all_tables))  # Read Note below
                for table in matching_tables:
                    if table not in dbt_project_yml['vars']['sources'][source]['tables']:
                        dbt_project_yml['vars']['sources'][source]['tables'].append({
                                                                                    'name': table})
    return dbt_project_yml


def inplace_or_copy(filetype):
    """
    Get user input for whether to replace or copy the existing sources.yml file.
    :param filetype: The file type (string) to replace or copy.
    :return: The string '_copy' if the user chose to copy, otherwise an empty string.
    """
    choice = ''
    while choice not in ('r', 'c'):
        choice = input(INPLACE_OR_COPY_HELPTEXT.format(filename=filetype))
    return '_copy' if choice == 'c' else ''


def main(
    dbt_project_path='', dbt_credentials_path='', project_id='',
    yaml=None, dbt_models_sources_path=''
):
    """
    The main function that runs the script.
    :param dbt_project_path: The file path of the dbt_project.yml file.
    :param dbt_credentials_path: The file path of the BigQuery credentials file.
    :param project_id: The project ID to set as the 'database' variable.
    :param yaml: The instance of the ruamel.yaml.YAML class.
    :param dbt_models_sources_path: The directory where the source files are located.
    """
    if not dbt_project_path:
        dbt_project_path = input(
            "Please enter the full path of the dbt_project.yml\
                                you'd like to modify:\n")
    if not dbt_credentials_path:
        dbt_credentials_path = input(CREDENTIALS_HELPTEXT)
    if not project_id:
        project_id = get_project_id(dbt_credentials_path)
    if not yaml:
        yaml = initialize_yaml()
    dbt_project_yml = load_dbt_project_yml(dbt_project_path, yaml)
    set_database(dbt_project_yml, project_id)
    client = get_client(dbt_credentials_path)
    if not dbt_models_sources_path:
        dbt_models_sources_path = input(
            "Please give the absolute path of the sources directory:\n")
    dbt_project_yml = add_sources_to_yml(
        dbt_project_yml, dbt_models_sources_path, client)
    copy_choice = inplace_or_copy("dbt_project")
    file, extension = os.path.splitext(dbt_project_path)
    with open(file + copy_choice + extension, 'w', encoding='utf-8') as f:
        yaml.dump(dbt_project_yml, f)


if __name__ == '__main__':
    main()
