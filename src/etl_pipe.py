'''
Class for running ETL pipeline. 

Started: 7/6/2023
Last Updated: 7/6/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import logging

#####################################################################################################################
## CLASS
#####################################################################################################################

class ETLPipeline():
    def __init__(self, database: object, fetcher: object, transformer: object, loader: object):
        """
        Initialize the ETL pipeline with database, fetcher, transformer, and loader.

        Parameters:
            database (Any): Database handler object.
            fetcher (Any): Data fetcher object.
            transformer (Any): Data transformer object.
            loader (Any): Data loader object.
        """
        self.db = database
        self.fetcher = fetcher
        self.transformer = transformer
        self.loader = loader
        self.logger = logging.getLogger(self.__class__.__name__)

    def pipe_data_to(self, table_name: str):
        """
        Execute the ETL pipeline and pipe data to the specified table.

        Parameters:
            table_name (str): The name of the table to load data into.
        """

        try:
            # Set up database
            logging.info("Creating database if not exists.")
            #self.db.create() 

            # Fetch data from the source
            logging.info("Fetching data.")
            raw_data = self.fetcher.execute()

            # Transform data
            logging.info("Transforming data.")
            transformed_data = self.transformer.execute(raw_data)

            # Load data
            logging.info(f"Loading data into table {table_name}.")
            self.loader.insert_data_to_sql(transformed_data, table_name)

            # Show data
            logging.info(f"Checking data in table {table_name}.")
            self.loader.check_table(table_name)

        except Exception as e:
            logging.error(f"Error in ETL pipeline: {e}")
            raise
    
    def __repr__(self):
        return (
            f"ETLPipeline(database={self.db.__class__.__name__}, "
            f"fetcher={self.fetcher.__class__.__name__}, "
            f"transformer={self.transformer.__class__.__name__}, "
            f"loader={self.loader.__class__.__name__})"
            )

#####################################################################################################################
## END
#####################################################################################################################
