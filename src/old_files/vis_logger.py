'''
Configures log for ETL.

Created: 7/23/24
Last Updated: 7/23/24
'''

# Libraries
import os
import logging

DIR_PATH = os.getcwd() + "/logs"
FILE_PATH = DIR_PATH + "/vis.log"

# Logging setup function
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,  # Set the log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log format
        handlers=[
            logging.FileHandler(FILE_PATH),  # Log messages to a file_path'
            logging.StreamHandler()  # Log messages to the console
        ]
    )

if __name__ == "__main__":
    setup_logger()