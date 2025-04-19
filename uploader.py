import os
from snowflake.snowpark import Session
import sys
import logging

# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": os.getenv("ACCOUNT_ID"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "ROLE": os.getenv("ROLE"),
        "DATABASE": os.getenv("DATABASE"),
        "SCHEMA": os.getenv("SCHEMA"),
        "WAREHOUSE": os.getenv("WAREHOUSE")
    }
    # creating snowflake session object
    snowpark_session = Session.builder.configs(connection_parameters).create()
    return snowpark_session  

# Traverse a directory and collect file info
def traverse_directory(directory: str, file_extension: str):
    file_names = []
    partition_dirs = []
    local_paths = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                full_path = os.path.abspath(os.path.join(root, file))
                file_names.append(file)
                # relative to the base directory
                relative_dir = os.path.relpath(root, directory)
                partition_dirs.append(relative_dir)
                local_paths.append(full_path)

    return file_names, partition_dirs, local_paths

# Upload files to Snowflake stage
def upload_files(file_names, partition_dirs, local_paths, stage_location, file_type):
    session = get_snowpark_session()
    for i, file in enumerate(file_names):
        full_stage_path = f"{stage_location}/{file_type}/{partition_dirs[i]}"
        logging.info(f"Uploading {file} to {full_stage_path}")
        result = session.file.put(
            local_paths[i],
            full_stage_path,
            auto_compress=False,
            overwrite=True,
            parallel=10
        )
        logging.info(f"Result: {result[0].status}")

# Main
def main():
    base_path = "data/sales"
    stage_location = "@sales_dwh.source.my_internal_stg"

    for ext in ['.csv', '.parquet', '.json']:
        names, dirs, paths = traverse_directory(base_path, ext)
        file_type = ext.replace('.', '')  # csv, parquet, json
        upload_files(names, dirs, paths, stage_location, file_type)

if __name__ == "__main__":
    main()
