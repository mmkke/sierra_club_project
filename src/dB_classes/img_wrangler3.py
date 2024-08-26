'''
Helper class for TransformData class. Downloads images from Googe Drive and inserts to Image table in DB. 

This version uses multithreading for faster download time.

Started: 8/26/2024
Last Updated: 8/26/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import logging
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.img_dict = {}

    def get_download_links(self, image_links):
        """
        Convert Google Drive links to direct download links and store them in img_dict.

        Parameters:
            image_links (list): List of image links to be processed.
        """
        try:
            existing_image_ids = pd.read_sql("SELECT photo_id FROM photos", self.engine)['photo_id'].tolist()
            self.logger.info(f"Retrieved {len(existing_image_ids)} existing image IDs from the database.")

            for image_link in image_links:
                image_id = image_link.split('=')[1]
                download_url = f"https://drive.google.com/uc?export=download&id={image_id}"

                if image_id in existing_image_ids:
                    self.logger.info(f"Image {image_id} already exists in the database. Skipping download.")
                    self.img_dict[image_id] = None
                else:
                    self.img_dict[image_id] = download_url
                    self.logger.info(f"Added {image_id} with download URL to img_dict.")

            self.logger.info("All image download links processed successfully.")

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while retrieving download links: {e}")

    def download_image(self, image_id, download_url, session):
        """
        Download a single image and insert it into the database.

        Parameters:
            image_id (str): ID of the image.
            download_url (str): Direct download URL of the image.
            session: SQLAlchemy session object.
        """
        try:
            if download_url is None:
                return False  # Image already exists in the database

            with requests.Session() as req_session:
                response = req_session.get(download_url, stream=True, timeout=10)
                if response.status_code == 200:
                    self.logger.info(f"Image {image_id} downloaded successfully.")
                    self.insert_image(session=session, image_id=image_id, image_data=response.content)
                    return True
                else:
                    self.logger.error(f"Failed to download image {image_id}. HTTP Status Code: {response.status_code}")
                    return False

        except requests.RequestException as e:
            self.logger.error(f"Failed to download image {image_id}. Network error: {e}")
            return False

    def get_images_and_insert_to_db(self):
        """
        Download images using the URLs in img_dict and insert them into the database.
        """
        try:
            db_session = self.Session()

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_image_id = {
                    executor.submit(self.download_image, image_id, download_url, db_session): image_id
                    for image_id, download_url in self.img_dict.items()
                }

                for future in as_completed(future_to_image_id):
                    image_id = future_to_image_id[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error occurred while processing image {image_id}: {e}")

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while downloading images: {e}")

        finally:
            db_session.close()

    def insert_image(self, session, image_id, image_data):
        """
        Insert the downloaded image into the database.

        Parameters:
            session (obj): SQLAlchemy session object.
            image_id (str): Unique identifier for the image.
            image_data (bytes): Binary data of the image.
        """
        try:
            photo = Photo(photo_id=image_id, photo=image_data)
            session.add(photo)
            session.commit()
            self.logger.info(f"Image {image_id} inserted successfully.")

        except Exception as e:
            session.rollback()
            self.logger.error(f"An error occurred while inserting image {image_id}: {e}")

    def execute(self, image_links):
        """
        Process the list of image links by downloading and inserting the images into the database.

        Parameters:
            image_links (list): List of image links to be downloaded and inserted.
        """
        self.get_download_links(image_links)
        self.get_images_and_insert_to_db()

        return self.img_dict.keys()