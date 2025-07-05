#!/usr/bin/env python3
"""
Test script to check a single Numbeo URL with the health care scraper
"""
import sys
from pathlib import Path
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.scrapers.health_care_scraper import HealthCareScraper
from src.scrapers.property_investment_scraper import PropertyInvestmentScraper
from src.scrapers.climate_scraper import ClimateScraper
from src.utils.file_saver import FileSaver

def test_scraper():
    """Test the climate scraper on Memphis, TN et print les CSV générés"""
    print("Testing Climate Scraper...")
    
    # Configure logging to show INFO+ in console
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    
    scraper = ClimateScraper()
    url = "https://www.numbeo.com/climate/in/Memphis"
    
    print(f"Testing URL: {url}")
    
    # Test scraping
    tables = scraper.scrape_category(url, "Memphis", "United States")
    print(f"Found {len(tables)} tables")
    for i, df in enumerate(tables):
        print(f"\nTable {i+1}:")
        print(f"  Caption: {df.get('table_caption', ['Unknown'])[0] if not df.empty else 'Unknown'}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Rows: {len(df)}")
        if not df.empty:
            print("  Data:")
            print(df.to_string(index=False))
    # Sauvegarde des CSV
    file_saver = FileSaver()
    saved_files = file_saver.save_category_data("Memphis", "United States", "climate", tables)
    print("\nCSV files created:")
    for f in saved_files:
        print(f"  {f}")

if __name__ == "__main__":
    test_scraper() 