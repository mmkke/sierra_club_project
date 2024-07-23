'''
Configures log for ETL.

Created: 7/23/24
Last Updated: 7/23/24
'''

# Libraries
import logging

# Logging setup function
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,  # Set the log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log format
        handlers=[
            logging.FileHandler("etl.log"),  # Log messages to a file named 'etl.log'
            logging.StreamHandler()  # Log messages to the console
        ]
    )

if __name__ == "__maine__":
    setup_logger()