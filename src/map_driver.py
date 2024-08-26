'''
Maps methane leaks and creates an HTML file for each city in the database.

Started: 7/12/2024
Last Updated: 8/26/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import sys
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Modules
#####################################################################################################################

from log_class import Log
from mapper_class import leakMapper

#####################################################################################################################
## Parameters
#####################################################################################################################

DATABASE = "methane_project_DB.db"
DB_FOLDER_PATH = current_dir / "data"
PATH_TO_DB = str(DB_FOLDER_PATH / DATABASE)
print(f"Path to DB: {PATH_TO_DB}")

#####################################################################################################################
## Main
#####################################################################################################################

def main():
# Configure logging
    DEBUG = False
    try:
        log_folder = current_dir / "logs"
        log_folder.mkdir(exist_ok=True)
        file_path = log_folder / "vis.log"
        vis_log = Log(file_path=file_path, stream=True)
        vis_log.configure()
        vis_log.debug_mode(enable_debug=DEBUG)
    except Exception as e:
        logging.error(f"Failed to configure logging: {e}", exc_info=True)
        sys.exit(1)

    # Query the database for a list of unique cities
    try:
        engine = create_engine(f'sqlite:///{PATH_TO_DB}')
        query = "SELECT DISTINCT city FROM measurements;"
        cities_df = pd.read_sql_query(query, engine)
        cities = cities_df['city'].tolist()
    except Exception as e:
        logging.error(f"Failed to query the database: {e}", exc_info=True)
        sys.exit(1)

    # Generate maps for each city
    for city in cities:
        try:
            # Create map object
            city_map = leakMapper(PATH_TO_DB, city)
            city_map.create_map()

            # Save map to HTML folder
            path_to_map = os.path.join(os.getcwd(), 'html')
            city_map.save_map(path_to_save_html=path_to_map)

            # Open map in web browser
            city_map.open_map()

        except Exception as e:
            logging.error(f"Failed to process city {city}: {e}", exc_info=True)
            continue  # Continue with the next city even if an error occurs

#####################################################################################################################
## END
#####################################################################################################################
if __name__ == '__main__':
    main()

