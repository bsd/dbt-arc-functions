#!/usr/bin/env python
# coding: utf-8
# TODO: Make source_regex_mappings into an external json file

import os
import re
import json

import ruamel.yaml
from google.cloud import bigquery
from google.oauth2 import service_account

source_regex_mappings = {
    'frakture_twitter_paidmedia.yml':{
        'schema':'src_frakture',
        'tables':[r'^twitter_[A-Za-z0-9]{3}_message$',
                r'^twitter_[A-Za-z0-9]{3}_ad_summary_by_date$'
               ]
    },
    'frakture_google_ads_paidmedia.yml':{
        'schema':'src_frakture',
        'tables':[r'^google_ads_[A-Za-z0-9]{3}_message$',
                r'^google_ads_[A-Za-z0-9]{3}_ad_summary_by_date$'
               ]
    },
    'frakture_facebook_paidmedia.yml':{
        'schema':'src_frakture',
        'tables':[r'^facebook_bizman_[A-Za-z0-9]{3}_message$',
                r'^facebook_bizman_[A-Za-z0-9]{3}_ad_summary_by_date$'
               ]
    },
    'frakture_everyaction_email.yml':{
        'schema':'src_frakture',
        'tables':[r'^everyaction_[A-Za-z0-9]{3}_email_summary$',
                r'^everyaction_[A-Za-z0-9]{3}_message$'
               ]
    },
}

credentials_helptext = """
If you'd like to know how to generate a credentials json go here: 
https://docs.getdbt.com/tutorial/setting-up#generate-bigquery-credentials

We need to use your Big Query Credentials to access the database and build out sources.

Please enter the absolute location of your credentials json file:
"""

inplace_or_copy_helptext = """
Would you like to (r)eplace the existing {filename}.yml or make a (c)opy named {filename}_copy.yml?
Enter r for replace or c for copy:
"""

def load_dbt_project_yml(dbt_project):
    with open(dbt_project, 'r') as f:
        content = f.read()
        return yaml.load(content)

def set_database(dbt_project_yml,project_id):
    variables = {'database': project_id}
    if 'vars' in dbt_project_yml:
        dbt_project_yml['vars']['database'] = project_id
    else:
        dbt_project_yml['vars']= variables

def get_project_id(credentials_path):
    with open(credentials_path,'r') as f:
        return json.load(f)['project_id']

def get_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id,)


def add_sources_to_yml(dbt_project_yml,sources_directory,client):
    if 'sources' not in dbt_project_yml['vars']:
        dbt_project_yml['vars']['sources'] = {}
    for source_yml in os.listdir(sources_directory):
        print(f"Working on {source_yml}")
        if source_yml in source_regex_mappings:
            source,_ = os.path.splitext(source_yml)

            if source not in dbt_project_yml['vars']['sources']:
                dbt_project_yml['vars']['sources'][source]={}
            if 'tables' not in dbt_project_yml['vars']['sources'][source]:
                dbt_project_yml['vars']['sources'][source]['tables']=[]
            query = f"SELECT table_name FROM `{source_regex_mappings[source_yml]['schema']}.INFORMATION_SCHEMA.TABLES`"
            query_job = client.query(query)
            all_tables = [row[0] for row in query_job]
            for table_regex in source_regex_mappings[source_yml]['tables']:
                r = re.compile(table_regex)
                matching_tables = list(filter(r.match, all_tables)) # Read Note below
                for table in matching_tables:
                    if table not in dbt_project_yml['vars']['sources'][source]['tables']:
                        dbt_project_yml['vars']['sources'][source]['tables'].append(table)
    return dbt_project_yml

def inplace_or_copy(filetype):
    choice = ''
    while choice not in ('r', 'c'):
        choice = input(inplace_or_copy_helptext.format(filename=filetype))
    return '_copy' if choice == 'c' else ''

if __name__ == '__main__':
    dbt_project = input("Please enter the full path of the dbt_project.yml you'd like to modify:\n")
    credentials_path = input(credentials_helptext)
    project_id = get_project_id(credentials_path)
    project_id_underscore = project_id.replace('-', '_')
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    dbt_project_yml = load_dbt_project_yml(dbt_project)
    set_database(dbt_project_yml,project_id)
    client = get_client(credentials_path)
    sources_directory = input("Please give the absolute path of the sources directory:\n")
    dbt_project_yml = add_sources_to_yml(dbt_project_yml,sources_directory,client)
    copy_choice = inplace_or_copy("dbt_project")
    file, extension = os.path.splitext(dbt_project)
    with open(file + copy_choice + extension, 'w') as f:
        yaml.dump(dbt_project_yml, f)