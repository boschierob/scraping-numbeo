"""
Configuration settings for Numbeo scraping project
"""
import os
from pathlib import Path
from datetime import datetime

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "datas"
OUTPUT_DIR = BASE_DIR / "output"

# URLs
BASE_URL = "https://www.numbeo.com"
QUALITY_OF_LIFE_URL = f"{BASE_URL}/quality-of-life"

# Categories to scrape
CATEGORIES = {
    "quality_of_life": "Quality of Life",
    "crime": "Crime",
    "cost_of_living": "Cost of Living", 
    "health_care": "Health Care",
    "climate": "Climate",
    "property_investment": "Property Investment",
    "traffic": "Traffic",
    "pollution": "Pollution"
}

# Scraping settings
REQUEST_DELAY = 2  # seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Table selectors
TABLE_SELECTORS = {
    "default": "table_builder_with_value_explanation",
    "traffic": ["table_builder_with_value_explanation", "data_wide_table"],
    "cost_of_living": "data_wide_table",
    "property_investment": ["table_indices", "data_wide_table"],
    "climate": "all"  # All tables plus tempChartDiv
}

# File naming
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
CSV_EXTENSION = ".csv"

# Database settings (for future MySQL import)
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "numbeo_data",
    "user": "root",
    "password": ""
}

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = BASE_DIR / "logs" / "scraping.log"

# Validation settings
MIN_TABLE_ROWS = 3  # Minimum rows for a valid table
MIN_CATEGORIES_SUCCESS = 3  # Minimum categories that must succeed

def get_output_folder():
    """Generate timestamped output folder name"""
    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    return OUTPUT_DIR / f"scraping_{timestamp}"

def ensure_directories():
    """Ensure all necessary directories exist"""
    directories = [DATA_DIR, OUTPUT_DIR, LOG_FILE.parent]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True) 