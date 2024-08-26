#!/usr/bin/env python3

"""
This script maps methane leaks in a given city arg and creates an HTML file for visualization.

Started: 8/26/2024
Last Updated: 8/26/2024
"""
 
#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import sys
from pathlib import Path
import argparse
import logging
import pandas as pd
from sqlalchemy import create_engine

#####################################################################################################################
## PATHING
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## MODULES
#####################################################################################################################

# Import custom modules
from leak_mapper import leakMapper

# Add the directory containing the module to sys.path
module_path = os.path.abspath(os.path.join('src', 'dB_classes'))
if module_path not in sys.path:
    sys.path.append(module_path)

from log_class import Log
from db_manager import LeakDB

#####################################################################################################################
## PARAMETERS
#####################################################################################################################

# Database configuration
DATABASE = "methane_project_DB.db"
DB_FOLDER_PATH = current_dir / "data"
PATH_TO_DB = str(DB_FOLDER_PATH / DATABASE)
print(f"Path to DB: {PATH_TO_DB}")

#####################################################################################################################
## MAIN FUNCTION
#####################################################################################################################

def main():
    """
    Main function to generate an HTML map for the given city.
    """

    try:
        # Query the database for a list of unique cities
        engine = create_engine(f'sqlite:///{PATH_TO_DB}')
        query = "SELECT DISTINCT city FROM measurements;"
        cities_df = pd.read_sql_query(query, engine)
        available_cities = cities_df['city'].tolist()
    except Exception as e:
        logging.error(f"Failed to query the database: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Get command-line arguments
        parser = argparse.ArgumentParser(description="Generate an HTML map for the given city.")
        parser.add_argument("city", type=str, help="The city to map.")
        args = parser.parse_args()
        city_name = args.city.strip()
        print(f"Provided city name: {city_name}")
    except Exception as e:
        logging.error(f"Error parsing arguments: {e}", exc_info=True)
        sys.exit(1)

    # Validate city name
    while True:
        if city_name not in available_cities:
            print("Requested city not available. Please choose from the following list:")
            print("\n".join(available_cities))
            city_name = input('Enter city name: ')
        else:
            print(f"Proceeding with city: {city_name}")
            break  # Exit the loop if a valid city is found

    try:
        # Configure logging
        DEBUG = False
        log_folder = current_dir / "logs"
        log_folder.mkdir(exist_ok=True)
        file_path = log_folder / "vis.log"
        vis_log = Log(file_path=file_path, stream=True)
        vis_log.configure()
        vis_log.debug_mode(enable_debug=DEBUG)
    except Exception as e:
        logging.error(f"Failed to configure logging: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Create the map object
        city_map = leakMapper(PATH_TO_DB, city_name)
        city_map.create_map()
    except Exception as e:
        logging.error(f"Failed to create map for {city_name}: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Save the map to the HTML folder
        path_to_map = os.path.join(os.getcwd(), 'html')
        os.makedirs(path_to_map, exist_ok=True)
        city_map.save_map(path_to_save_html=path_to_map)
    except Exception as e:
        logging.error(f"Failed to save map for {city_name}: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Open the map in a web browser
        city_map.open_map()
    except Exception as e:
        logging.error(f"Failed to open map for {city_name}: {e}", exc_info=True)
        sys.exit(1)

#####################################################################################################################
## END
#####################################################################################################################

if __name__ == '__main__':
    main()