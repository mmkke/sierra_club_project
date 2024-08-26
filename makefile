# Define directories and file patterns
LOG_FILES = $(wildcard logs/*.log)
API_DATA_DIR = $(wildcard data/api_data/*.csv)
DB_FILES = $(wildcard data/*.db)
HTML_FILES = $(wildcard html/*.html)
ETL = src/dB_classes/etl_run.py
MAP_ALL = src/viz_classes/leak_mapper.py
CREATE_MAP = sierra_club_project/src/viz_classes/create_map.py

# Default target
all: help

# Help target
help:
	@echo "Makefile for cleaning directory"
	@echo ""
	@echo "Usage:"
	@echo " make run-etl          - Runs src/dB_classes/etl_run.py, creating database"
	@echo " make run-map-all       - Runs src/viz_classes/leak_mapper.py, creating HTML maps for all cities and opening in browser"
	@echo " make run-all          - Runs run-etl and run-map-all"
	@echo " make run-create-map   - Runs sierra_club_project/src/viz_classes/create_map.py to create and open map for specified CITY argument"
	@echo ""
	@echo "Cleaning:"
	@echo "  make clean           - Remove .log files, everything in /data/api_data, .db files, and .html files"
	@echo "  make clean-log       - Remove all .log files"
	@echo "  make clean-api       - Remove everything in /data/api_data"
	@echo "  make clean-db        - Remove all .db files"
	@echo "  make clean-html      - Remove all .html files"

# Create database and run ETL pipeline
run-etl:
	@echo "Creating LeakDB and populating tables from Google Sheets..."
	python $(ETL)
	@echo "ETL pipeline completed."

# Create map objects
run-map-all:
	@echo "Creating maps and opening HTML in browser..."
	python $(MAP_ALL)
	@echo "Map creation completed."

# Run create_map.py with CITY argument
run-create-map:
ifeq ($(strip $(CITY)),)
	$(error CITY argument is required. Usage: make run-create-map CITY=<city_name>)
endif
	@echo "Running create_map.py for city: $(CITY)"
	python $(CREATE_MAP) $(CITY)
	@echo "Map created for city: $(CITY)"

# Run ETL and create maps for all available cities
run-all: run-etl run-mapper
	@echo "Running all tasks..."

# Clean .log files
clean-log:
	@echo "Removing .log files..."
	rm -f $(LOG_FILES)
	@echo "Log files cleaned."

# Clean everything in /data/api_data directory
clean-api:
	@echo "Removing everything in /data/api_data directory..."
	rm -rf $(API_DATA_DIR)
	@echo "API data directory cleaned."

# Clean .db files
clean-db:
	@echo "Removing .db files..."
	rm -f $(DB_FILES)
	@echo "Database files cleaned."

# Clean .html files
clean-html:
	@echo "Removing .html files..."
	rm -f $(HTML_FILES)
	@echo "HTML files cleaned."

# Clean all specified files and directories
clean: clean-log clean-api clean-db clean-html
	@echo "Cleaned specified files and directories."

# Clean all including other temporary files
clean-all: clean
	@echo "Removing additional temporary files..."
	rm -rf build dist *.egg-info
	@echo "All temporary files cleaned."