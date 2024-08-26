'''
Driver for running ETL pipeline. 

Started: 7/6/2023
Last Updated: 7/19/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
from pathlib import Path

import pandas as pd

#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Modules
#####################################################################################################################

from db_manager import LeakDB
from fetch_data_from_api import FetchData
from transformer import TransformData
from loader import LoadData
from etl_pipe import ETLPipeline
from log_class import Log

#####################################################################################################################
## Parameters
#####################################################################################################################

DATABASE = "methane_project_DB.db"
DB_FOLDER_PATH = current_dir / "data"
SQL_PREFIX = "sqlite:///"
PATH_TO_DB = SQL_PREFIX + str(DB_FOLDER_PATH / DATABASE)
CREDENTIALS_PATH = 'credentials.json'
GOOGLE_SHEET_ID = '1oJ2wAGYLkEd8VeKinrbiAmOwjlZpYqONL09P4LZ01po'
RANGE_NAME = 'Form Responses 1!A1:G'
TABLE_NAME = "measurements"
DEBUG = False

#####################################################################################################################
## Main
#####################################################################################################################

def main():

    # Check if the folder exists, if not create it
    if not DB_FOLDER_PATH.exists():
        DB_FOLDER_PATH.mkdir(parents=True, exist_ok=True)
    print(f"Directory {DB_FOLDER_PATH} exists: {DB_FOLDER_PATH.exists()}")

    # Setup logging
    file_path = current_dir / "logs/etl.log"
    etl_log = Log(file_path=file_path, stream=True)
    etl_log.configure()
    etl_log.debug_mode(enable_debug=DEBUG)

    # Init databaseand etl objects
    database = LeakDB(PATH_TO_DB)
    fetcher = FetchData(CREDENTIALS_PATH, GOOGLE_SHEET_ID, RANGE_NAME)
    transformer = TransformData(PATH_TO_DB)
    loader = LoadData(PATH_TO_DB)

    # Create pipeline
    pipe = ETLPipeline(database,
                       fetcher,
                       transformer,
                       loader)
    # Run pipe
    pipe.pipe_data_to(TABLE_NAME)
    
    # Check db contents
    database.print_all_tables_and_values()

    # Query DB
    #query="SELECT * FROM photos"
    #df = database.query_db(query)
    #print(df)

#####################################################################################################################
## END
#####################################################################################################################
if __name__ == '__main__':
    main()

#####################################################################################################################
## END
#####################################################################################################################