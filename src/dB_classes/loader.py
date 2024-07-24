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
from sqlalchemy import create_engine, select, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

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
        self.engine: Optional[Engine] = create_engine(self.path_to_db)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = None

    def connect(self):
        """Establish a connection to the SQLite database using SQLAlchemy."""
        if self.engine is None:
            self.engine = create_engine(self.path_to_db)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.logger.info("Database connection established.")

    def close_connection(self):
        """Close the connection to the SQLite database."""
        if self.session:
            self.session.close()
            self.session = None
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
            self.logger.info(f"Inserting data into table '{table_name}'.")

            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)

            with self.engine.connect() as connection:
                existing_timestamps = pd.read_sql(f"SELECT timestamp FROM {table_name}", connection)['timestamp'].tolist()
                self.logger.info(f"Existing timestamps: {existing_timestamps}")

                new_data = df[~df['timestamp'].isin(existing_timestamps)]
                if new_data.empty:
                    self.logger.info(f"No new data to insert for table '{table_name}'.")
                else:
                    new_data.to_sql(table_name, self.engine, if_exists='append', index=False)
                    self.logger.info(f"Inserted {len(new_data)} new records into table '{table_name}' at {self.engine.url}.")
                    self.logger.info(f"Inserted contents: \n{new_data}")

        except Exception as e:
            self.logger.error(f"Failed to insert data into the SQL table '{table_name}' at {self.engine.url}: {e}")
            if self.session:
                self.session.rollback()
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
            self.logger.info(f"Querying table '{table_name}' at {self.engine.url}.")

            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)

            with self.engine.connect() as connection:
                stmt = select(table)
                result = connection.execute(stmt)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                self.logger.info(f"Table '{table_name}' columns: {df.columns.tolist()}")
                self.logger.info(f"Table '{table_name}' contents:\n{df}")

        except Exception as e:
            self.logger.error(f"Failed to query SQL table '{table_name}' at {self.engine.url}: {e}")
            raise
        finally:
            self.close_connection()

#####################################################################################################################
## END
#####################################################################################################################