"""
City data loading functionality
"""
import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Optional
from ..config.settings import DATA_DIR

logger = logging.getLogger(__name__)

class CityLoader:
    """Handles loading and managing city data"""
    
    def __init__(self, csv_file: str = "cities.csv"):
        self.csv_file = DATA_DIR / csv_file
        self.cities = []
        
    def load_cities(self) -> List[Dict]:
        """
        Load cities from CSV file and build URLs automatically
        
        Returns:
            List of city dictionaries with 'country', 'city', 'url' keys
        """
        try:
            if not self.csv_file.exists():
                logger.error(f"City file not found: {self.csv_file}")
                return []
                
            df = pd.read_csv(self.csv_file)
            logger.info(f"Loaded {len(df)} cities from {self.csv_file}")
            
            # Convert DataFrame to list of dictionaries
            self.cities = df.to_dict('records')
            
            # Build URLs for cities that don't have them, with fallback
            for city in self.cities:
                if 'url' not in city or not city['url']:
                    city['url'] = self._find_valid_city_url(
                        city.get('city', ''),
                        city.get('country', ''),
                        city.get('region', None)
                    )
                    logger.debug(f"Built (fallback) URL for {city.get('city')}: {city['url']}")
            
            # Validate required columns
            required_columns = ['country', 'city']
            for city in self.cities:
                missing_columns = [col for col in required_columns if col not in city]
                if missing_columns:
                    logger.warning(f"City {city.get('city', 'Unknown')} missing columns: {missing_columns}")
                    
            return self.cities
            
        except Exception as e:
            logger.error(f"Error loading cities: {e}")
            return []
    
    def get_city_by_name(self, city_name: str, country_name: str = None) -> Optional[Dict]:
        """
        Get specific city by name and optionally country
        
        Args:
            city_name: Name of the city
            country_name: Optional country name for disambiguation
            
        Returns:
            City dictionary or None if not found
        """
        for city in self.cities:
            if city.get('city', '').lower() == city_name.lower():
                if country_name is None or city.get('country', '').lower() == country_name.lower():
                    return city
        return None
    
    def get_cities_by_country(self, country_name: str) -> List[Dict]:
        """
        Get all cities for a specific country
        
        Args:
            country_name: Name of the country
            
        Returns:
            List of city dictionaries
        """
        return [city for city in self.cities 
                if city.get('country', '').lower() == country_name.lower()]
    
    def validate_city_url(self, city: Dict) -> bool:
        """
        Validate that a city has a valid URL
        
        Args:
            city: City dictionary
            
        Returns:
            True if URL is valid, False otherwise
        """
        url = city.get('url', '')
        return url.startswith('http') and 'numbeo.com' in url
    
    def get_total_cities(self) -> int:
        """Get total number of loaded cities"""
        return len(self.cities)
    
    def _find_valid_city_url(self, city_name: str, country_name: str, region: str = None) -> str:
        """
        Try different Numbeo URL formats for a city and return the first valid one.
        """
        from ..config.settings import BASE_URL
        import requests
        from bs4 import BeautifulSoup

        def clean(val):
            return str(val).replace(" ", "-")

        candidates = [
            f"{BASE_URL}/quality-of-life/in/{clean(city_name)}",
            f"{BASE_URL}/quality-of-life/in/{clean(city_name)}-{clean(country_name)}"
        ]
        if region and str(region).strip():
            candidates.append(f"{BASE_URL}/quality-of-life/in/{clean(city_name)}-{clean(region)}-{clean(country_name)}")

        headers = {"User-Agent": "Mozilla/5.0"}
        for url in candidates:
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    if soup.find("table") or soup.find("h1"):
                        return url
            except Exception:
                continue
        return candidates[0]  # fallback: retourne la première même si non valide
    
    def _build_city_url(self, city_name: str, country_name: str) -> str:
        """
        Build Numbeo URL for a city based on name and country
        
        Args:
            city_name: Name of the city
            country_name: Name of the country
            
        Returns:
            Numbeo URL for the city
        """
        try:
            from ..config.settings import BASE_URL
            
            # Clean and format names for URL
            city_clean = self._clean_name_for_url(city_name)
            # country_clean = self._clean_name_for_url(country_name)  # plus utilisé
            
            # Numbeo URL format: https://www.numbeo.com/quality-of-life/in/City
            city_identifier = city_clean
            url = f"{BASE_URL}/quality-of-life/in/{city_identifier}"
            
            return url
            
        except Exception as e:
            logger.error(f"Error building URL for {city_name}, {country_name}: {e}")
            return ""
    
    def _clean_name_for_url(self, name: str) -> str:
        """
        Clean name for use in URL
        
        Args:
            name: Original name
            
        Returns:
            Cleaned name suitable for URL
        """
        # Remove special characters and replace spaces with hyphens
        import re
        
        # Preserve first letter case, convert rest to lowercase
        if name:
            first_letter = name[0]
            rest_of_name = name[1:].lower()
            cleaned = first_letter + rest_of_name
        else:
            cleaned = name.lower()
        
        # Replace spaces and special characters with hyphens
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned)
        cleaned = re.sub(r'\s+', '-', cleaned)
        
        # Remove multiple hyphens
        cleaned = re.sub(r'-+', '-', cleaned)
        
        # Remove leading/trailing hyphens
        cleaned = cleaned.strip('-')
        
        return cleaned 