"""
Configues log.

Created: 7/25/24
Updated:
"""

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import logging
from logging import Handler

#####################################################################################################################
## CLASS
#####################################################################################################################
    
"""
Configures logging.

Created: 7/25/24
Updated:
"""

#####################################################################################################################
## LIBRARIES
#####################################################################################################################

import os
import logging
from logging import Handler

#####################################################################################################################
## CLASS
#####################################################################################################################

class Log:
    def __init__(self, file_path: str = None, stream: bool = True, level: object = logging.INFO, log_format: str = None):
        """
        Initializes the Log class with the given parameters.

        Parameters:
        ----------
        file_path : str, optional
            The file path to the log file (default is a log file in the current working directory).
        stream : bool, optional
            Whether to include a stream handler (default is True).
        level : object, optional
            The logging level (default is logging.DEBUG).
        log_format : str, optional
            The format for log messages (default is '%(asctime)s - %(name)s - %(levelname)s - %(message)s').
        """
        if file_path is None:
            file_path = os.path.join(os.getcwd(), "log.log")
        self.file_path = file_path
        self.level = level
        self.format = log_format if log_format else '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.handlers = [logging.FileHandler(file_path)]
        if stream:
            self.handlers.append(logging.StreamHandler())

    def stream_on(self):
        """
        Adds a stream handler to the handlers if it doesn't already exist.

        Returns:
        -------
        self : Log
            The instance of the Log class.
        """
        if not any(isinstance(handler, logging.StreamHandler) for handler in self.handlers):
            self.handlers.append(logging.StreamHandler())
        return self

    def stream_off(self):
        """
        Removes the stream handler from the handlers.

        Returns:
        -------
        self : Log
            The instance of the Log class.
        """
        self.handlers = [handler for handler in self.handlers if not isinstance(handler, logging.StreamHandler)]
        return self

    def add_handler(self, handler):
        """
        Adds a custom handler to the handlers list.

        Parameters:
        ----------
        handler : logging.Handler
            The logging handler to add.

        Returns:
        -------
        self : Log
            The instance of the Log class.
        """
        if not isinstance(handler, Handler):
            raise TypeError("Handler must be an instance of logging.Handler")
        self.handlers.append(handler)
        return self

    def debug_mode(self, enable_debug=True):
        """
        Toggles debug mode on or off.

        Parameters:
        ----------
        enable_debug : bool
            If True, sets logging level to DEBUG; otherwise, sets it to INFO.

        Returns:
        -------
        self : Log
            The instance of the Log class.
        """
        if enable_debug:
            self.level = logging.DEBUG
        else:
            self.level = logging.INFO

        for handler in self.handlers:
            handler.setLevel(self.level)
        
        logging.getLogger().setLevel(self.level)  # Set root logger level as well
        return self

    def configure(self):
        """
        Configures the logging with the current settings.

        Returns:
        -------
        self : Log
            The instance of the Log class.
        """
        logging.basicConfig(
            level=self.level,
            format=self.format,
            handlers=self.handlers
        )
        return self