'''
Class for loading transformed data in into DB.

Started: 6/23/2023
Last Updated: 7/23/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import logging
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

#####################################################################################################################
## CLASS
#####################################################################################################################

class LoadData:
    def __init__(self, path_to_db: str):
        """
        Initialize the LoadData class.

        Parameters:
            path_to_db (str): Path to the database.
        """
        self.path_to_db = path_to_db
        self.engine: Optional[Engine] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def connect(self):
        """Establish a connection to the SQLite database using SQLAlchemy."""
        if self.engine is None:
            self.engine = create_engine(self.path_to_db)
            self.logger.info("Database connection established.")

    def close_connection(self):
        """Close the connection to the SQLite database."""
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.logger.info("Database connection closed.")

    def insert_data_to_sql(self, df: pd.DataFrame, table_name: str):
        """
        Insert data into the specified SQL table.

        Parameters:
            df (pd.DataFrame): DataFrame containing the data to be inserted.
            table_name (str): Name of the SQL table.
        """
        try:
            self.connect()
            self.logger.info(f"Inserting data into table {table_name}.")

            with self.engine.connect() as connection:
                # Check existing records
                existing_timestamps = pd.read_sql(f"SELECT timestamp FROM {table_name}", connection)['timestamp'].tolist()
                new_data = df[~df['timestamp'].isin(existing_timestamps)]

                # Insert new data
                if not new_data.empty:
                    new_data.to_sql(table_name, connection, if_exists='append', index=False)
                    self.logger.info(f"Inserted {len(new_data)} new records into the database.")
                else:
                    self.logger.info("No new records to insert.")

        except Exception as e:
            self.logger.error(f"Failed to insert data into the SQL table: {e}")
            raise
        finally:
            self.close_connection()

    def check_table(self, table_name: str):
        """
        Query and display the contents of the specified SQL table.

        Parameters:
            table_name (str): Name of the SQL table.
        """
        try:
            self.connect()
            self.logger.info(f"Querying table {table_name}.")

            with self.engine.connect() as connection:
                # Read the table into a pandas DataFrame
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)

                # Display the DataFrame
                self.logger.info(f"Table {table_name} columns: {df.columns.tolist()}")
                self.logger.info(f"Table {table_name} contents:\n{df}")

        except Exception as e:
            self.logger.error(f"Failed to query SQL table: {e}")
            raise
        finally:
            self.close_connection()
    

#####################################################################################################################
## END
#####################################################################################################################