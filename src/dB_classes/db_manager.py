'''
Class for creating and managing methane database.

Started: 6/23/2023
Last Updated: 7/23/2024
'''
#####################################################################################################################
## Libraries
#####################################################################################################################
import logging

import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, Float, Boolean, LargeBinary, ForeignKey, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

#####################################################################################################################
## Classes
#####################################################################################################################


Base = declarative_base()

class City(Base):
    """
    ORM class representing the 'cities' table.
    """
    __tablename__ = 'cities'
    city_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String, unique=True)
    county = Column(String)
    state = Column(String, default="MAINE")
    utility_provider = Column(String, ForeignKey('utility_providers.company_name'))

class UtilityProvider(Base):
    """
    ORM class representing the 'utility_providers' table.
    """
    __tablename__ = 'utility_providers'
    provider_id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String, unique=True)
    mailing_address = Column(String)
    phone_number = Column(String)
    region = Column(String)

class Measurement(Base):
    """
    ORM class representing the 'measurements' table.
    """
    __tablename__ = 'measurements'
    measurement_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String, ForeignKey('cities.city'))
    methane_level = Column(Float)
    leak = Column(Boolean)
    type_of_infrastructure = Column(String)
    photo_id = Column(String, ForeignKey('photos.photo_id'))
    latitude = Column(Float)
    longitude = Column(Float)
    volunteer = Column(String)
    timestamp = Column(TIMESTAMP, unique=True)

class Photo(Base):
    """
    ORM class representing the 'photos' table.
    """
    __tablename__ = 'photos'
    photo_id = Column(String, primary_key=True)
    photo = Column(LargeBinary)

class Volunteer(Base):
    """
    ORM class representing the 'volunteers' table.
    """
    __tablename__ = 'volunteers'
    volunteer_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String)
    last_name = Column(String)
    city = Column(String, ForeignKey('cities.city'))
    initials = Column(String)

class LeakDB:
    """
    Class to manage the Gas Leak Project database using SQLAlchemy.
    """
    def __init__(self, path_to_db):
        """
        Initialize the LeakDB class.

        Parameters:
            path_to_db (str): Path to the SQLite database file.
        """
        self.path_to_db = path_to_db
        self.engine = create_engine(self.path_to_db)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)
        Base.metadata.create_all(self.engine)
        self.logger.info("Database tables created successfully.")

    def add_volunteer(self, first_name, last_name, initials, city):
        """
        Add a volunteer to the database.

        Parameters:
            first_name (str): First name of the volunteer.
            last_name (str): Last name of the volunteer.
            initials (str): Initials of the volunteer.
            city (str): City of the volunteer.
        """
        session = self.Session()
        try:
            volunteer = Volunteer(first_name=first_name, last_name=last_name, initials=initials, city=city)
            session.add(volunteer)
            session.commit()
            self.logger.info(f"Volunteer {first_name} {last_name} added successfully.")
        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while adding a volunteer: {e}")
        finally:
            session.close()

    def add_utility_provider(self, company_name, mailing_address, phone_number, region):
        """
        Add a utility provider to the database.

        Parameters:
            company_name (str): Name of the utility provider.
            mailing_address (str): Mailing address of the utility provider.
            phone_number (str): Phone number of the utility provider.
            region (str): Region of the utility provider.
        """
        session = self.Session()
        try:
            provider = UtilityProvider(company_name=company_name, mailing_address=mailing_address, phone_number=phone_number, region=region)
            session.add(provider)
            session.commit()
            self.logger.info(f"Utility provider {company_name} added successfully.")
        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while adding a utility provider: {e}")
        finally:
            session.close()

    def add_city(self, city_name, city_county, city_state, city_utility_provider):
        """
        Add a city to the database.

        Parameters:
            city_name (str): Name of the city.
            city_county (str): County of the city.
            city_state (str): State of the city.
            city_utility_provider (str): Utility provider of the city.
        """
        session = self.Session()
        try:
            city = City(city=city_name, county=city_county, state=city_state, utility_provider=city_utility_provider)
            session.add(city)
            session.commit()
            self.logger.info(f"City {city_name} added successfully.")
        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while adding a city: {e}")
        finally:
            session.close()

    def add_image(self, idx, image_data):
        """
        Add an image to the database.

        Parameters:
            idx (str): Unique identifier for the image.
            image_data (bytes): Binary data of the image.
        """
        session = self.Session()
        try:
            photo = Photo(photo_id=idx, photo=image_data)
            session.add(photo)
            session.commit()
            self.logger.info(f"Image inserted into the database with photo_id: {idx}")
        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while adding an image: {e}")
        finally:
            session.close()

    def insert_data_to_sql(self, df, table_class):
        """
        Insert data from a DataFrame into a specified table.

        Parameters:
            df (pandas.DataFrame): DataFrame containing the data to insert.
            table_class (Base): The table class corresponding to the table.
        """
        session = self.Session()
        try:
            df.to_sql(table_class.__tablename__, con=self.engine, if_exists='append', index=False)
            self.logger.info("Data inserted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to insert data into the SQL table: {e}")
            raise
        finally:
            session.close()

    def print_all_tables_and_values(self):
        """
        Print all tables and their values.
        """
        session = self.Session()
        try:
            tables = Base.metadata.tables.keys()
            for table in tables:
                self.logger.info(f"Table: {table}")
                result = session.execute(f"SELECT * FROM {table}")
                columns = result.keys()
                self.logger.info(f"Columns: {columns}")
                for row in result:
                    self.logger.info(row)
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            session.close()

    def query_db(self, query):
        """
        Execute a generic query on the database.

        Parameters:
            query (str): SQL query to execute.

        Returns:
            pandas.DataFrame: DataFrame containing the query results.
        """
        session = self.Session()
        try:
            results = session.execute(text(query))
            results_df = pd.DataFrame(results.fetchall(), columns=results.keys())
            return results_df
        except Exception as e:
            self.logger.error(f"An error occurred while executing the query: {str(e)}")
        finally:
            session.close()

#####################################################################################################################
## END
#####################################################################################################################
