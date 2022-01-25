import first_run_dbt_project_and_profiles_fixer
import create_or_update_standard_models
import compile_sources
import add_dependencies
import delete_schemas
from os import path


def main():
    print("Running first_run_dbt_project_and_profiles_fixer.py\n")
    [dbt_project_path,
     project_id,
     yaml,
     dbt_credentials_path,
     username] = first_run_dbt_project_and_profiles_fixer.main()
    dbt_base_path = path.dirname(dbt_project_path)
    dbt_models_path = path.join(dbt_base_path, "models")
    print("\nRunning create_or_update_standard_models.py\n")
    create_or_update_standard_models.main(dbt_models_path)
    dbt_models_sources_path = path.join(dbt_models_path, "sources")
    print("\nRunning compile_sources.py\n")
    compile_sources.main(dbt_project_path, dbt_credentials_path, project_id, yaml, dbt_models_sources_path)
    print("\nRunning delete_schemas.py\n")
    delete_schemas.main(dbt_credentials_path,username)
    print("\nRunning add_dependencies.py\n")
    add_dependencies.main(dbt_base_path)
    print("\nWow, it all actually ran! Congratulations, you should be able to run this repo now.\n")


if __name__ == '__main__':
    main()
