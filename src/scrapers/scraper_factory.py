"""
Factory for creating category-specific scrapers
"""
import logging
from typing import Dict, Type
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ScraperFactory:
    """Factory for creating category-specific scrapers"""
    
    def __init__(self):
        self._scrapers = {}
        self._register_scrapers()
    
    def _register_scrapers(self):
        """Register all available scrapers"""
        try:
            from .quality_of_life_scraper import QualityOfLifeScraper
            from .traffic_scraper import TrafficScraper
            from .cost_of_living_scraper import CostOfLivingScraper
            from .property_investment_scraper import PropertyInvestmentScraper
            from .climate_scraper import ClimateScraper
            from .crime_scraper import CrimeScraper
            from .health_care_scraper import HealthCareScraper
            from .pollution_scraper import PollutionScraper
            
            self._scrapers = {
                "quality_of_life": QualityOfLifeScraper,
                "traffic": TrafficScraper,
                "cost_of_living": CostOfLivingScraper,
                "property_investment": PropertyInvestmentScraper,
                "climate": ClimateScraper,
                # Use CrimeScraper for crime
                "crime": CrimeScraper,
                "health_care": HealthCareScraper,
                "pollution": PollutionScraper,
            }
            
        except ImportError as e:
            logger.warning(f"Some scrapers not available: {e}")
            # Fallback to generic scraper for all categories
            from .generic_scraper import GenericScraper
            self._scrapers = {
                "quality_of_life": GenericScraper,
                "traffic": GenericScraper,
                "cost_of_living": GenericScraper,
                "property_investment": GenericScraper,
                "climate": GenericScraper,
                "crime": GenericScraper,
                "health_care": GenericScraper,
                "pollution": GenericScraper,
            }
    
    def get_scraper(self, category: str) -> BaseScraper:
        """
        Get the appropriate scraper for a category
        
        Args:
            category: Category name
            
        Returns:
            Scraper instance
        """
        scraper_class = self._scrapers.get(category)
        if not scraper_class:
            logger.warning(f"No scraper found for category '{category}', using generic scraper")
            from .generic_scraper import GenericScraper
            scraper_class = GenericScraper
        
        return scraper_class()
    
    def get_available_categories(self) -> list:
        """Get list of available categories"""
        return list(self._scrapers.keys())