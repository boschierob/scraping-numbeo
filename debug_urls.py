#!/usr/bin/env python3
"""
Debug script to test URL generation
"""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.data.city_loader import CityLoader
from src.utils.url_builder import URLBuilder

def main():
    print("Testing URL generation...")
    
    # Load cities
    city_loader = CityLoader()
    cities = city_loader.load_cities()
    
    print(f"Loaded {len(cities)} cities:")
    for city in cities:
        print(f"  {city['city']}, {city['country']}: {city['url']}")
    
    # Test URL building
    url_builder = URLBuilder()
    
    for city in cities:
        city_name = city['city']
        country_name = city['country']
        city_url = city['url']
        
        print(f"\nTesting URLs for {city_name}, {country_name}:")
        print(f"  Base URL: {city_url}")
        
        category_urls = url_builder.get_all_category_urls(city_url)
        for category, url in category_urls.items():
            print(f"  {category}: {url}")

if __name__ == "__main__":
    main() 