'''
Maps methane leaks.

Started: 7/12/2023
Last Updated: 7/19/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import sys
from pathlib import Path
import requests
import json
import folium
import base64
import numpy as np
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import io
from PIL import Image
import sqlite3
from shapely.geometry import Point
from sqlalchemy import create_engine
import webbrowser



#####################################################################################################################
## Pathing
#####################################################################################################################

# Get the current working directory
current_dir = Path.cwd()
print(f"Current working directory: {current_dir}")

#####################################################################################################################
## Modules
#####################################################################################################################
# Add the directory containing the module to sys.path
module_path = os.path.abspath(os.path.join('src/dB_classes'))
sys.path.append(module_path)

from manage_methane_db import MethaneDB


#####################################################################################################################
## Pathing
#####################################################################################################################

DATABASE = "methane_project_DB"
DB_FOLDER_PATH = current_dir / "data"
PATH_TO_DB = str(DB_FOLDER_PATH / DATABASE)
print(f"Path to DB: {PATH_TO_DB}")


#####################################################################################################################
## Classes
#####################################################################################################################

class leakMapper():
    def __init__(self, path_to_db, table_name, city):
        self.path_to_db = path_to_db
        self.table = table_name
        self.city = city
        self.engine = create_engine(f'sqlite:///{path_to_db}')
        self.df = None
        self.gdf = None
        self.map = None
        self.map_name = f"{city}_maine_map.html"
        self.imgdf = pd.read_sql_table('photos', self.engine)

    def set_df(self):
        '''Queries DB to create Dataframe.'''
        try:
            query = f"SELECT * FROM {self.table} WHERE city = '{self.city}'"
            self.df = pd.read_sql_query(query, self.engine)
            print(self.df)

        except Exception as e:
            print('Error retrieving data from database.', e)
            raise
        finally:
            #self.close_connection()
            pass

    def set_gdf(self):
        '''Creates GeoPandas Dataframe by adding geomtery.'''

        if self.df is None:
            print('DataFrame is not set. Call set_df() first.')
            return
        
        try:
            #self.connect()
            self.gdf = gpd.GeoDataFrame(self.df, 
                                    geometry=gpd.points_from_xy(self.df['longitude'], self.df['latitude'])
                                    )
            print(self.gdf)

        except Exception as e:
            print('Error creating GeoPandas dataframe.', e)
            raise
        finally:
            #self.close_connection()
            pass

    def get_image(self, photo_id):
        """
        Convert image BLOB to base64 string.
        """
        try:
            print('Getting image...')
            image_blob = self.imgdf[self.imgdf['photo_id'] == photo_id]['photo'].values[0]
            image_base64 = base64.b64encode(image_blob).decode('utf-8')
            image_html = f'<img src="data:image/jpeg;base64,{image_base64}" width="150" height="100">'
            return image_html
        
        except IndexError:
            print(f"No matching image found for photo_id == {photo_id}")
            return "<p>No image available</p>"
    
    def set_base_map(self):

        if self.gdf is None:
            print('GeoDataFrame is not set. Call set_gdf() first.')
            return

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

    def add_markers(self, layers=True):
        '''Adds markers with pop-up windows for all data points.'''
        
        # ITeratire through records and add pop-up for each
        for idx, row in self.gdf.iterrows():
            location = [row['latitude'], row['longitude']]
            # filter out any records without valid lat/long
            if not np.isnan(np.array(location)).any():

                # get image from db
                image_html = self.get_image(row['photo_id'])
                html = f"""
                        <h4>Methane reading: {row['methane_level']} ppm </h4>
                        <h4>Date/time recorded: {row['timestamp']} </h4>
                        <h4>Infastructure type: {row['type_of_infrastructure']} </h4>
                        <h4>Picture:</h4>
                        {image_html}
                        """
                # create popup
                popup = folium.Popup(html, lazy=True)

                if layers:

                    # Create feature groups for different marker layers
                    layer_nonzero = folium.FeatureGroup(name='Non Zero Results')
                    layer_zero = folium.FeatureGroup(name='Zero Results')

                    # Add markers to specific layers based on your condition
                    if row['leak']: 
                        folium.Marker(location=location, popup=popup).add_to(layer_nonzero)
                    else:
                        folium.Marker(location=location, popup=popup).add_to(layer_zero)

                    # Add the feature groups to the map
                    layer_nonzero.add_to(self.map)
                    layer_zero.add_to(self.map)

                    # Add layer control to the map
                    folium.LayerControl().add_to(self.map)
                    
                else:
                    folium.Marker(location=location, popup=popup).add_to(self.map)
            else:
                # IF NAN IN LAT/LONG SKIP RECORD
                pass



    def save_map(self):
        try:
            # Save the map as an HTML file
            print(f'Saving map to local dir: {os.getcwd()}')
            self.map.save(self.map_name)
            print(f"Map has been saved as {self.map_name}")
        except Exception as e:
            print("Error saving map:", e)

    def open_map(self):

        # Open the HTML file in the default web browser
        webbrowser.open('file://' + os.getcwd() + '/' + self.map_name)
    
    def create_map(self):
        self.set_df()
        self.set_gdf()
        self.set_base_map()
        self.add_markers()
        self.save_map()
        self.open_map()

        
#####################################################################################################################
## Main
#####################################################################################################################

def main():
    TABLE_NAME = 'measurements'
    CITY = 'Portland'
    portland_map = leakMapper(PATH_TO_DB, TABLE_NAME, CITY)
    portland_map.create_map()

#####################################################################################################################
## END
#####################################################################################################################
if __name__ == '__main__':
    main()

#####################################################################################################################
## END
#####################################################################################################################