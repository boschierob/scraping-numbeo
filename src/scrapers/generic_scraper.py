"""
Generic scraper for Numbeo categories
"""
import pandas as pd
import logging
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class GenericScraper(BaseScraper):
    """Generic scraper for categories not specifically implemented (squelette)"""
    def __init__(self):
        super().__init__()
        self.category_name = "generic"
    def scrape_category(self, url: str, city_name: str, country_name: str):
        logger.info(f"[SQUELETTE] Scraping generic category for {city_name}, {country_name}")
        return [] 