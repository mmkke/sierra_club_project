'''
Class for connecting to google sheets api and update db.

Started: 6/15/2023
Last Updated: 7/23/2024
'''

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import logging
import re
from typing import Optional, Callable

import numpy as np
import pandas as pd

#####################################################################################################################
## Modules
#####################################################################################################################

from img_wrangler3 import ImageWrangler

#####################################################################################################################
## CLASS
#####################################################################################################################
class TransformData:
    def __init__(self, path_to_db: str):
        """
        Initialize the TransformData class.

        Parameters:
            path_to_db (str): Path to the database.
        """
        self.df: Optional[pd.DataFrame] = None
        self.path_to_db = path_to_db
        self.image_wrangler = ImageWrangler(path_to_db)
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_df(self, df: pd.DataFrame):
        """Set the DataFrame to be transformed."""
        self.df = df
        return self

    def get_df(self) -> Optional[pd.DataFrame]:
        """Get the transformed DataFrame."""
        return self.df

    def validate_data(self):
        """Validate the DataFrame to ensure all necessary columns are present and contain valid data."""
        required_columns = ['coordinates', 'photo', 'methane_level', 'volunteer', 'timestamp']
        for column in required_columns:
            if column not in self.df.columns:
                self.logger.error(f"Missing required column: {column}")
                raise ValueError(f"Missing required column: {column}")
        self.logger.info("Data validation successful.")
        return self

    def reset(self, df: Optional[pd.DataFrame] = None):
        """Reset the DataFrame to its initial state or to a new state."""
        self.df = df
        self.logger.info("DataFrame has been reset.")
        return self

    def apply_custom_transformation(self, func: Callable[[pd.DataFrame], pd.DataFrame]):
        """Apply a custom transformation function to the DataFrame."""
        try:
            self.df = func(self.df)
            self.logger.info("Custom transformation applied successfully.")
        except Exception as e:
            self.logger.error(f"Failed to apply custom transformation: {e}")
            raise
        return self

    # def get_lat_and_long(self):
    #     """Extract and convert latitude and longitude from coordinates."""
    #     try:
    #         valid_mask = self.df['coordinates'].apply(self.is_valid_coord)
    #         self.df[['latitude', 'longitude']] = (
    #             self.df['coordinates'].where(valid_mask)
    #                 .str.replace(r"[()]", "", regex=True)
    #                 .str.split(",", expand=True)
    #         )
    #         self.df['latitude'] = self.df['latitude'].astype(float)
    #         self.df['longitude'] = self.df['longitude'].astype(float)
    #         self.df.loc[~valid_mask, ['latitude', 'longitude']] = np.nan
    #         self.df.drop(columns=['coordinates'], inplace=True)
    #     except Exception as e:
    #         self.logger.error(f"Failed to get latitude and longitude: {e}")
    #         raise
    #     return self

    # @staticmethod
    # def is_valid_coord(s: str) -> bool:
    #     """Check if a string is a valid coordinate."""
    #     pattern = r'^\s*\(?\s*-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?\s*\)?\s*$'
    #     return bool(re.match(pattern, s))

    def get_lat_and_long(self):
        """Extract and convert latitude and longitude from coordinates."""
        try:
            self.logger.debug("Starting to extract latitude and longitude.")
            valid_mask = self.df['coordinates'].apply(self.is_valid_coord)
            self.logger.debug(f"Valid coordinates mask: {valid_mask}")
            
            # Extract latitude and longitude while handling different formats
            self.df[['latitude', 'longitude']] = (
                self.df['coordinates'].where(valid_mask)
                    .str.replace(r"[()]", "", regex=True)
                    .str.extract(r'([+-]?\d+\.\d+)[°\s]*([NSns])?,?\s*([+-]?\d+\.\d+)[°\s]*([EWew])?')
                    .apply(self.convert_to_decimal, axis=1)
            )
            self.logger.debug(f"Extracted latitude and longitude: {self.df[['latitude', 'longitude']]}")
            
            self.df.loc[~valid_mask, ['latitude', 'longitude']] = np.nan
            self.df.drop(columns=['coordinates'], inplace=True)
            self.logger.debug("Dropped 'coordinates' column.")
        except Exception as e:
            self.logger.error(f"Failed to get latitude and longitude: {e}")
            raise
        return self

    @staticmethod
    def convert_to_decimal(row):
        """Convert the latitude and longitude values to decimal format."""
        lat, lat_dir, lon, lon_dir = row
        
        # Log the raw latitude and longitude values
        logging.debug(f"Raw latitude: {lat}, Direction: {lat_dir}, Raw longitude: {lon}, Direction: {lon_dir}")
        
        # Handle the latitude direction (N/S)
        if lat_dir in ['S', 's']:
            lat = -abs(float(lat))
        # else:
        #     lat = abs(float(lat))
        
        # Handle the longitude direction (E/W)
        if lon_dir in ['W', 'w']:
            lon = -abs(float(lon))
        # else:
        #     lon = abs(float(lon))
        
        logging.debug(f"Converted latitude: {lat}, Converted longitude: {lon}")
        return pd.Series([lat, lon])

    @staticmethod
    def is_valid_coord(s: str) -> bool:
        """Check if a string is a valid coordinate."""
        pattern = r"""
            ^\s*                      # Optional leading whitespace
            \(?                       # Optional opening parenthesis
            [+-]?\d+(\.\d+)?\s*°?\s*  # Latitude with optional degree symbol and whitespace
            [NSns]?,?\s*              # Optional N/S and comma
            [+-]?\d+(\.\d+)?\s*°?\s*  # Longitude with optional degree symbol and whitespace
            [EWew]?\)?\s*$            # Optional E/W, closing parenthesis, and trailing whitespace
        """
        match = bool(re.match(pattern, s, re.VERBOSE))
        logging.debug(f"Coordinate: {s}, Valid: {match}")
        return match

    def get_images(self):
        """Download images and replace photo URLs with photo IDs."""
        try:
            id_list = []
            for link in self.df['photo']:
                if link:
                    self.logger.info(f"Downloading image from {link}")
                    photo_id = self.image_wrangler.execute(link)
                    id_list.append(photo_id)
                else:
                    id_list.append(None)
            self.logger.info(f"'photo_id column: {id_list}")
            self.df['photo_id'] = pd.Series(id_list)
            self.df.drop(columns='photo', inplace=True)
        except Exception as e:
            self.logger.error(f"Failed to get images: {e}")
            raise
        return self
    
    def get_images2(self):
        """Download images and replace photo URLs with photo IDs."""
        try:
            link_list = self.df['photo'].tolist()
            id_list = self.image_wrangler.execute(link_list)
            self.logger.info(f"'photo_id column: {id_list}")
            self.df['photo_id'] = pd.Series(id_list)
            self.df.drop(columns='photo', inplace=True)
        except Exception as e:
            self.logger.error(f"Failed to get images: {e}")
            raise
        return self

    def get_leaks(self):
        """Add a leak column based on methane levels."""
        try:
            self.df['leak'] = self.df['methane_level'].astype(float) > 0
        except Exception as e:
            self.logger.error(f"Failed to get leaks: {e}")
            raise
        return self

    def get_volunteers(self):
        """Format volunteer initials to uppercase."""
        try:
            self.df['volunteer'] = self.df['volunteer'].str.upper()
        except Exception as e:
            self.logger.error(f"Failed to format volunteer initials: {e}")
            raise
        return self

    def format_timestamp(self):
        """Convert timestamp to ISO 8601 format."""
        try:
            self.df['timestamp'] = pd.to_datetime(
                self.df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.error(f"Failed to format timestamp: {e}")
            raise
        return self

    def lel_to_ppm(self):
        """Convert Lower Explosive Limit (LEL) to Parts per Million (PPM)."""
        try:
            self.df['methane_level'] = self.df['methane_level'].apply(
                lambda x: float(x) * 50000 * 0.01
            )
        except Exception as e:
            self.logger.error(f"Failed to convert LEL to PPM: {e}")
            raise
        return self

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the data transformation pipeline.

        Parameters:
            df (pd.DataFrame): The DataFrame to transform.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        try:
            return (self
                    .set_df(df)
                    .validate_data()
                    .format_timestamp()
                    .get_lat_and_long()
                    .get_images2()
                    .get_leaks()
                    .lel_to_ppm()
                    .get_volunteers()
                    .get_df())
        except Exception as e:
            self.logger.error(f"Execution failed: {e}")
            raise
#####################################################################################################################
## END
#####################################################################################################################