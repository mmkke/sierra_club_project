# Define directories and file patterns
LOG_FILES = $(wildcard *.log)
API_DATA_DIR = data/api_data/*
DB_FILES = $(wildcard *.db)
HTML_FILES = $(wildcard *.html)

# Default target
all: help

# Help target
help:
	@echo "Makefile for cleaning directory"
	@echo ""
	@echo "Usage:"
	@echo "  make clean     - Remove .log files, everything in /data/api_data, .db files, and .html files"
	@echo "  make clean-log - Remove all .log files"
	@echo "  make clean-api - Remove everything in /data/api_data"
	@echo "  make clean-db  - Remove all .db files"
	@echo "  make clean-html - Remove all .html files"

# Clean .log files
clean-log:
	@echo "Removing .log files..."
	rm -f $(LOG_FILES)
	@echo "Done."

# Clean everything in /data/api_data directory
clean-api:
	@echo "Removing everything in /data/api_data directory..."
	rm -rf $(API_DATA_DIR)
	@echo "Done."

# Clean .db files
clean-db:
	@echo "Removing .db files..."
	rm -f $(DB_FILES)
	@echo "Done."

# Clean .html files
clean-html:
	@echo "Removing .html files..."
	rm -f $(HTML_FILES)
	@echo "Done."

# Clean all specified files and directories
clean: clean-log clean-api clean-db clean-html
	@echo "Cleaned specified files and directories."

# Clean all including other temporary files
clean-all: clean
	@echo "Removing additional temporary files..."
	rm -rf build dist *.egg-info
	@echo "Done."