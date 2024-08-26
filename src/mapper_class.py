'''
Maps methane leaks and creates an HTML file for each city in the database.

Started: 7/12/2024
Last Updated: 8/26/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import base64
import io
import os
import sys
import webbrowser
import logging
from pathlib import Path

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
from PIL import Image
from shapely.geometry import Point
from sqlalchemy import create_engine

#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Modules
#####################################################################################################################

from db_manager import LeakDB
from log_class import Log

#####################################################################################################################
## Parameters
#####################################################################################################################

DATABASE = "methane_project_DB.db"
DB_FOLDER_PATH = current_dir / "data"
PATH_TO_DB = str(DB_FOLDER_PATH / DATABASE)
print(f"Path to DB: {PATH_TO_DB}")

#####################################################################################################################
## Classes
#####################################################################################################################

class leakMapper():
    def __init__(self, path_to_db, city, table_name='measurements'):
        self.path_to_db = path_to_db
        self.table = table_name
        self.city = city
        self.engine = create_engine(f'sqlite:///{path_to_db}')
        self.df = None
        self.gdf = None
        self.map = None
        self.map_name = f"{city}_maine_map.html"
        self.imgdf = pd.read_sql_table('photos', self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_df(self):
        """
        Queries the database to create a DataFrame.

        This function executes a SQL query to retrieve data from the database based on the 
        specified table and city, and stores the result in the instance variable `self.df`.

        Parameters:
            None

        Returns:
            None

        Raises:
            Exception: If there is an error retrieving data from the database.
        """
        try:
            # Construct the SQL query using the table and city attributes
            query = f"SELECT * FROM {self.table} WHERE city = '{self.city}'"
            
            # Execute the SQL query and store the result in a DataFrame
            self.df = pd.read_sql_query(query, self.engine)
            
            # Print the DataFrame for debugging purposes
            self.logger.info(self.df)

        except Exception as e:
            self.logger.error('Error retrieving data from database.', e)            
            raise

    def set_gdf(self):
        """
        Creates a GeoPandas DataFrame by adding geometry.

        This function converts the existing DataFrame (`self.df`) into a GeoPandas DataFrame 
        (`self.gdf`) by adding geometry based on longitude and latitude columns.

        Parameters:
            None

        Returns:
            None

        Raises:
            Exception: If there is an error creating the GeoPandas DataFrame.
        """
       
        # Check if the DataFrame is set
        if self.df is None:
            self.logger.info('DataFrame is not set. Call set_df() first.')
            return
        
        try:
            # Create a GeoPandas DataFrame with geometry based on longitude and latitude
            self.gdf = gpd.GeoDataFrame(self.df, 
                                        geometry=gpd.points_from_xy(self.df['longitude'], self.df['latitude'])
                                    )
            
            # Print the GeoPandas DataFrame for debugging purposes
            self.logger.info(self.gdf)

        except Exception as e:
            self.logger.error('Error creating GeoPandas dataframe.', e)
            raise
       
    def get_image(self, photo_id):
        """
        Retrieve an image from the DataFrame, compress it, convert to a base64 string, and generate HTML
        for embedding.

        Parameters:
        - photo_id: The ID of the photo to retrieve.

        Returns:
        - str: An HTML string to embed the base64 encoded image. If the photo_id is not found, returns an 
        HTML string indicating that no image is available.
        """
        try:
            # Print a message indicating the image retrieval process has started
            self.logger.info(f'Getting image: {photo_id}')
            
            # Retrieve the image blob corresponding to the given photo_id from the DataFrame
            image_blob = self.imgdf[self.imgdf['photo_id'] == photo_id]['photo'].values[0]
            
            # Compress the image blob and convert it to a base64 encoded string
            image_base64 = self.compress_image(image_blob)
            
            # Create an HTML string to embed the base64 encoded image with specified width and height
            image_html = f'<img src="data:image/jpeg;base64,{image_base64}" width="100" height="100">'
            
            # Return the generated HTML string
            return image_html
    
        except IndexError:
            # Print an error message if no matching image is found for the given photo_id
            self.logger.error(f"No matching image found for photo_id == {photo_id}")
    
    def compress_image(self, img_blob, img_size=(100,100), quality=100):
        """
        Compress an image from a blob to a thumbnail size and convert to base64 in memory.

        Parameters:
        - image_blob: bytes, the image data as a blob.
        - thumbnail_size: tuple, size of the thumbnail (width, height). Default is (100, 100).
        - quality: int, the quality of the saved image (1-100). Default is 85.

        Returns:
        - str, base64 encoded string of the compressed thumbnail image.
        """
        try:
            # Open the image from the blob
            img = Image.open(io.BytesIO(img_blob))
            
            # Create a thumbnail of the image with the specified size
            img.thumbnail(img_size)
            
            # Create a BytesIO object to save the compressed image in memory
            buffered = io.BytesIO()
            
            # Save the thumbnail image to the BytesIO object with specified quality
            img.save(buffered, format="JPEG", optimize=True, quality=quality)
            
            # Encode the BytesIO object's value to a base64 string
            img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            
            # Return the base64 encoded string of the compressed thumbnail image
            return img_base64
        
        except Exception as e:
            self.logger.error(f"Error in image compression: {e}")
        
    def set_base_map(self):
        """
        Sets base map by centering map on the mean of lat/long points.
        
        Parameters:
            None
        Returns:
            None
        """

        if self.gdf is None:
            self.logger.info('GeoDataFrame is not set. Call set_gdf() first.')
            return
        try:
            # Set the coordinate reference system (CRS) to WGS84 (EPSG:4326)
            self.gdf.set_crs(epsg=4326, inplace=True)

            # Convert any Timestamp columns to strings to avoid json serialization issues
            for col in self.gdf.columns:
                if pd.api.types.is_datetime64_any_dtype(self.gdf[col]):
                    self.gdf[col] = self.gdf[col].astype(str)

                # Initialize a folium map centered around the first point
                center = [self.gdf.geometry.y.mean(), self.gdf.geometry.x.mean()]
                self.map = folium.Map(location=center, 
                                    zoom_start=13, 
                                    control_scale=True,
                                    max_bounds=True)

            # Calculate the bounds
            min_lon, max_lon = self.gdf.geometry.x.min(), self.gdf.geometry.x.max()
            min_lat, max_lat = self.gdf.geometry.y.min(), self.gdf.geometry.y.max()
            bounds = [[min_lat, min_lon], [max_lat, max_lon]]

            # Fit the map to these bounds
            self.map.fit_bounds(bounds)

        except Exception as e:
            self.logger.error(f"Error setting base map layer:, {e}")

    def create_popup(self, row):
        """
        Create an HTML popup for a data row containing methane reading, timestamp, infrastructure type,
        and image.

        Parameters:
        - row (pandas.Series): A row of data containing the necessary information to create the popup.

        Returns:
        - folium.Popup: A Folium Popup object containing the formatted HTML content.
        - None: Returns None if an exception occurs during popup creation.
        """
        try:
            # Retrieve the image HTML from the database using the photo_id from the row
            image_html = self.get_image(row['photo_id'])

            # Format the HTML content for the popup
            html = f"""
                    <h6>Methane reading: ~{row['methane_level']} ppm </h6>
                    <h6>Date/time recorded: {row['timestamp']} </h6>
                    <h6>Infrastructure type: {row['type_of_infrastructure']} </h6>
                    <h6>Picture:</h6>
                    {image_html}
                    """

            # Create and return a Folium Popup object with the formatted HTML content
            return folium.Popup(html, lazy=True)
        
        except Exception as e:
            # Print an error message if an exception occurs
            self.logger.error(f"Error creating popup: {e}")
            
        # Ensure None is returned if an exception occurs
        return None

    def add_markers(self, layers=True):
        '''
        Adds markers with pop-up windows for all data points.

        Parameters:
            -layers: boo, if True, markers will be add to different layers depending on whether 
            record is nonzero. Default is True. 

        Returns:
            None
            '''
        try:
            if layers:
                # Create feature groups for different marker layers
                layer_nonzero = folium.FeatureGroup(name='Non Zero Results')
                layer_zero = folium.FeatureGroup(name='Zero Results')
            
            # Iterrate through records and add pop-up for each
            for idx, row in self.gdf.iterrows():
                # get lcoation
                location = [row['latitude'], row['longitude']]
                if not np.isnan(location).any():
                    popup = self.create_popup(row)
                    if layers:
                        # Add markers to specific layers based on your condition
                        if row['leak']: 
                            icon = folium.Icon(color='red', prefix='fa', icon="fa-fire-flame-simple")
                            layer = layer_nonzero
                        else:
                            icon = folium.Icon(color='green', prefix='fa',  icon="fa-fire-flame-simple")
                            layer = layer_zero
                    else:
                        icon = folium.Icon(color='blue', prefix='fa',  icon='fa-fire-flame-simple')
                        layer = self.map
                    
                    folium.Marker(location=location, popup=popup, icon=icon).add_to(layer)

            if layers:
                # Add the feature groups and layer control to the map
                layer_nonzero.add_to(self.map)
                layer_zero.add_to(self.map)
                folium.LayerControl().add_to(self.map)
        except Exception as e:
            # Print an error message if an exception occurs
            self.logger.error("Error adding markers:", e)

    def save_map(self, path_to_save_html=None):
        """
        Saves the map HTML object to a specified directory or the current working directory.

        Parameters:
            path_to_save_html (str): Optional. The path to save the HTML file. If not provided, 
            saves to the current working directory.

        Returns:
            None
        """

        try:
            # Determine the save path
            if path_to_save_html is not None:

                # Aheck if dir exists, if not make it
                os.makedirs(path_to_save_html, exist_ok=True)

                # Append map name to dir path
                save_path = path_to_save_html + '/' + self.map_name

            else:
                # Set path to CWD
                save_path = os.path.join(os.getcwd(), self.map_name)
            
            # Save the map as an HTML file
            self.logger.info(f'Saving map to: {save_path}')
            self.map.save(save_path)
            self.logger.info(f"Map has been saved as {save_path}")

        except Exception as e:
            # Print an error message if an exception occurs
            self.logger.error("Error saving map:", e)

    def open_map(self):
        '''
        Opens map HTML file in default web browser.

        Parameters:
            None

        Returns:
            None
        '''
        try:
            # Construct the file URL for the map HTML file
            file_url = 'file://' + os.path.join(os.getcwd() + '/html/', self.map_name)
            
            # Open the HTML file in the default web browser
            webbrowser.open(file_url)
            self.logger.info('Opening map in browser...')
        
        except Exception as e:
            # Print an error message if an exception occurs
            self.logger.error("Error opening map:", e)
    
    def create_map(self):
        self.set_df()
        self.set_gdf()
        self.set_base_map()
        self.add_markers()


#####################################################################################################################
## END
#####################################################################################################################


