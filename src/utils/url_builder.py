"""
URL building utilities for Numbeo scraping
"""
import logging
from urllib.parse import urljoin, urlparse
from typing import Dict, List
from ..config.settings import BASE_URL, CATEGORIES

logger = logging.getLogger(__name__)

class URLBuilder:
    """Handles URL construction for different Numbeo categories"""
    
    def __init__(self):
        self.base_url = BASE_URL
        self.categories = CATEGORIES
    
    def build_category_url(self, city_url: str, category: str) -> str:
        """
        Build URL for a specific category page
        
        Args:
            city_url: Base city URL
            category: Category name (e.g., 'crime', 'cost_of_living')
            
        Returns:
            Full URL for the category page
        """
        try:
            # Parse the city URL to get the city identifier
            parsed_url = urlparse(city_url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            # Find the city identifier (usually the last part of the path)
            city_identifier = path_parts[-1] if path_parts else ""
            
            if not city_identifier:
                logger.error(f"Could not extract city identifier from URL: {city_url}")
                return ""
            
            # Build category-specific URL
            category_url = self._get_category_path(category, city_identifier)
            full_url = urljoin(self.base_url, category_url)
            
            logger.debug(f"Built URL for {category}: {full_url}")
            return full_url
            
        except Exception as e:
            logger.error(f"Error building URL for category {category}: {e}")
            return ""
    
    def _get_category_path(self, category: str, city_identifier: str) -> str:
        """
        Get the URL path for a specific category
        
        Args:
            category: Category name
            city_identifier: City identifier from URL
            
        Returns:
            URL path for the category
        """
        category_paths = {
            "quality_of_life": f"quality-of-life/in/{city_identifier}",
            "crime": f"crime/in/{city_identifier}",
            "cost_of_living": f"cost-of-living/in/{city_identifier}",
            "health_care": f"health-care/in/{city_identifier}",
            "climate": f"climate/in/{city_identifier}",
            "property_investment": f"property-investment/in/{city_identifier}",
            "traffic": f"traffic/in/{city_identifier}",
            "pollution": f"pollution/in/{city_identifier}"
        }
        
        return category_paths.get(category, "")
    
    def extract_city_identifier(self, city_url: str) -> str:
        """
        Extract city identifier from a Numbeo city URL
        
        Args:
            city_url: Full city URL
            
        Returns:
            City identifier string
        """
        try:
            parsed_url = urlparse(city_url)
            path_parts = parsed_url.path.strip('/').split('/')
            return path_parts[-1] if path_parts else ""
        except Exception as e:
            logger.error(f"Error extracting city identifier: {e}")
            return ""
    
    def validate_url(self, url: str) -> bool:
        """
        Validate if a URL is properly formatted
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc])
        except Exception:
            return False
    
    def get_all_category_urls(self, city_url: str) -> Dict[str, str]:
        """
        Generate URLs for all categories for a given city
        
        Args:
            city_url: Base city URL
            
        Returns:
            Dictionary mapping category names to URLs
        """
        urls = {}
        for category in self.categories.keys():
            url = self.build_category_url(city_url, category)
            if url:
                urls[category] = url
        
        return urls
    
    def is_numbeo_url(self, url: str) -> bool:
        """
        Check if URL is a valid Numbeo URL
        
        Args:
            url: URL to check
            
        Returns:
            True if it's a Numbeo URL
        """
        try:
            parsed = urlparse(url)
            return 'numbeo.com' in parsed.netloc
        except Exception:
            return False 