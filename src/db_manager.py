'''
Class for creating and managing methane database.

Started: 6/23/2023
Last Updated: 7/26/2024
'''
#####################################################################################################################
## Libraries
#####################################################################################################################
import logging

import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, Float, Boolean, LargeBinary, ForeignKey, TIMESTAMP, select
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import IntegrityError


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
    provider_id = Column(Integer, primary_key=True)
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
        self.initialize_data()


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

    def insert_data_to_sql(self, dataframe, table_class):
        """
    Inserts data from a DataFrame into a specified SQL table, skipping duplicates.
    
    :param dataframe: DataFrame containing the data to be inserted.
    :param model_class: The SQLAlchemy model class corresponding to the table.
    """
        Session = sessionmaker(bind=self.engine)
        session = Session()
    
        for index, row in dataframe.iterrows():
            data = row.to_dict()
            
            # Check if the record already exists
            existing_record = session.query(table_class).filter_by(**data).first()
            if existing_record:
                self.logger.info(f"Skipping duplicate record: {data}")
                continue
            
            # If the record doesn't exist, insert it
            try:
                session.add(table_class(**data))
                session.commit()
            except IntegrityError as e:
                session.rollback()
                self.logger.error(f"IntegrityError: {e}")
            except Exception as e:
                session.rollback()
                self.logger.error(f"An error occurred: {e}")
        
        session.close()

    def print_all_tables_and_values(self):
        """
        Print all tables and their values.
        """
        session = self.Session()
        try:
            tables = Base.metadata.tables.values()
            for table in tables:
                self.logger.info(f"Table: {table}")
                stmt = select(table)
                result = session.execute(stmt)
                columns = table.columns.keys()
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

    def initialize_data(self):
        """
        Reads CSV files and inserts data into the respective tables.
        """
        try:
            volunteers_df = pd.read_csv("data/project_data/volunteers.csv")
            volunteers_df.columns = volunteers_df.columns.str.strip()  # Strip any leading/trailing whitespace
            volunteers_df.index.name = 'volunteer_id'
            self.logger.info(f'Volunteers DataFrame: \n{volunteers_df}')
            self.insert_data_to_sql(volunteers_df, Volunteer)
            
            utilities_df = pd.read_csv("data/project_data/utilities.csv")
            utilities_df.columns = utilities_df.columns.str.strip()  # Strip any leading/trailing whitespace
            utilities_df.index.name = ''
            self.logger.info(f'Utilties DataFrame: \n{utilities_df}')
            self.insert_data_to_sql(utilities_df, UtilityProvider)
            
            cities_df = pd.read_csv("data/project_data/cities.csv")
            cities_df.columns = cities_df.columns.str.strip()  # Strip any leading/trailing whitespace
            self.logger.info(f'Cities DataFrame: \n{cities_df}')
            self.insert_data_to_sql(cities_df, City)
        except FileNotFoundError as e:
            print(f"File not found: {e}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

#####################################################################################################################
## END
#####################################################################################################################
