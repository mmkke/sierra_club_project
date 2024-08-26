'''
Helper class for TransformData class. Downloads images from Googe Drive and inserts to Image table in DB.  

Started: 7/23/2024
Last Updated: 7/23/2024
Retired: 8/26/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import logging
import requests

import pandas as pd
from sqlalchemy import create_engine, Column, String, LargeBinary
from sqlalchemy.orm import sessionmaker
from io import BytesIO
from PIL import Image

from db_manager import Photo, Base  


#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = os.getcwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Class
#####################################################################################################################

class ImageWrangler:
    """
    Takes image download links, downloads images, inserts them into the images table in the DB, and returns the image_id.
    """
    def __init__(self, path_to_db):
        """
        Initialize the ImageWrangler class.

        Parameters:
            path_to_db (str): Path to the SQLite database file.
        """
        self.path_to_db = path_to_db
        self.engine = create_engine(self.path_to_db)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

    def download_image(self, image_link):
        """
        Download image from the provided link.

        Parameters:
            image_link (str): Link to the image to be downloaded.

        Returns:
            bool: True if the image was downloaded successfully, False otherwise.
        """
        try:
            # Convert Google Drive link to direct download link
            image_id = image_link.split('=')[1]
            self.logger.info(f"image_id: {image_id}")
            download_url = f"https://drive.google.com/uc?export=download&id={image_id}"

            # Check if image already exists in db
            existing_image_ids = pd.read_sql(f"SELECT photo_id FROM photos", self.engine)['photo_id'].tolist()
            if image_id in existing_image_ids:
                self.logger.info(f"Image {image_id} already exists in table.")
                return image_id, None
            
            # Download image data 
            with requests.Session() as session: # Use a session for connection pooling
                response = session.get(download_url, stream=True)
                if response.status_code == 200:
                    self.logger.info(f'Image {image_id} downloaded successfully.')
                    return image_id, response.content
                else:
                    self.logger.error(f"Failed to download image. HTTP Status Code: {response.status_code}")
                    return image_id, None
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to download image. Network error: {e}")
            return image_id, None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            return image_id, None

    def insert_image(self, image_id, image_data, check_id=False):
        """
        Insert the downloaded image into the database.

        Parameters:
            image_id (str): Unique identifier for the image.
            image_data (bytes): Binary data of the image.
            check_id (bool): If True checks if image_id exists in database table. Default is False.
        """
        session = self.Session()
        try:
            # Check if image already exists in db
            if check_id == True:
                existing_image = session.query(Photo).filter_by(photo_id=image_id).first()
                if existing_image:
                    self.logger.info(f"Image {image_id} already exists in the database.")
                    return
            
            # Insert the image as a BLOB with the unique ID
            photo = Photo(photo_id=image_id, photo=image_data)
            session.add(photo)
            session.commit()
            self.logger.info(f"Image {image_id} inserted successfully.")

        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while inserting the image: {e}")
        finally:
            session.close()

    def execute(self, image_link):
        """
        Download and insert image into the database.

        Parameters:
            image_link (str): Link to the image to be downloaded.

        Returns:
            str: The image_id if the image was inserted successfully, None otherwise.
        """
        image_id, image_data = self.download_image(image_link)
        self.logger.info(f"Download image returned: \n  photo_id: {bool(image_id)} \n  photo: {bool(image_data)}")
        
        if image_id and image_data:
            self.insert_image(image_id, image_data)
            return image_id
        
        return image_id
        
#####################################################################################################################
## END
#####################################################################################################################
