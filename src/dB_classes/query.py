'''
Allows for command line queries of the database. 

Started: 7/23/2024
Last Updated: 7/23/2024
'''
#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, exec

#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Parameters
#####################################################################################################################

DATABASE = "methane_project_DB"
DB_FOLDER_PATH = current_dir / "data"
SQL_PREFIX = "sqlite:///"
PATH_TO_DB = SQL_PREFIX + str(DB_FOLDER_PATH / DATABASE)
print(f"Path to Database: {PATH_TO_DB}")

#####################################################################################################################
## Functions
#####################################################################################################################

def execute_query(engine, query):
    """
    Execute a SQL query using the provided engine.
    
    Inputs:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
        query (str): SQL query to execute.
    
    Returns:
        pandas.DataFrame: Query results.
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except exec.SQLAlchemyError as e:
        print(f"Error executing query: {e}")
        raise
    
#####################################################################################################################
## Maine
#####################################################################################################################

def main():

    # Set up argparser
    parser = argparse.ArgumentParser(description="Query a database using SQLAlchemy.")

    # Take command-line argument as SQL query
    parser.add_argument('query', type=str, help='SQL query to execute')
    parser.add_argument('--db_url', type=str, default=None, help='Database connection string') # optional, defaults to hardcoded param if second arg not given
    args = parser.parse_args()

    # Connect to db via SQLAlchemy
    if args.db_url is not None:
        engine = create_engine(args.db_url)
    else:
        engine = create_engine(PATH_TO_DB)

    # Query db and return reults to a dataframe
    result_df = execute_query(engine, args.query)
    print(result_df)

#####################################################################################################################
## END
#####################################################################################################################
if __name__ == "__main__":
    main()