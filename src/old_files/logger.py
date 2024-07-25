'''
Configures log for ETL.

Created: 7/23/24
Last Updated: 7/23/24
'''

# Libraries
import os
import logging

DIR_PATH = os.getcwd() + "/logs"
FILE_PATH = DIR_PATH + "/etl.log"

# Logging setup function
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,  # Set the log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log format
        handlers=[
            logging.FileHandler(FILE_PATH),  # Log messages to a file named 'etl.log'
            logging.StreamHandler()  # Log messages to the console
        ]
    )

if __name__ == "__maine__":
    setup_logger()

'''
class Log():
    def __init__(self, file_path: str, stream: bool = True):
        self.level = logging.INFO
        self.format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        if stream:
            self.handlers = [logging.StreamHandler(), logging.FileHandler(file_path)]
        else:
            self.handlers = logging.FileHandler(file_path)
        self.file_path = file_path

    def stream_on(self):
        self.handlers.append(logging.StreamHandler())
        return self
    
    def stream_off(self):
        self.handlers.remove(logging.StreamHandler())
        return self
    
    def add_handler(self, handler):
        self.handler.append(handler)
        return self

    def configure(self):
        logging.basicConfig(
                            self.level,
                            self.format,
                            self.handlers
                            )
        return self
'''