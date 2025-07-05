"""
Cost of Living scraper for Numbeo
"""
import pandas as pd
import logging
import re
from typing import List
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CostOfLivingScraper(BaseScraper):
    """Scraper for Cost of Living category"""
    def __init__(self):
        super().__init__()
        self.category_name = "cost_of_living"
    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        logger.info(f"Scraping Cost of Living for {city_name}, {country_name}")
        try:
            soup = self.get_page(url)
            if not soup:
                logger.error(f"Failed to fetch page: {url}")
                return []
            if self._is_blocked_page(soup):
                logger.warning(f"Page appears to be blocked: {url}")
                self.save_debug_html(soup, f"blocked_{city_name}_{self.category_name}")
                return []
            tables = self._extract_cost_of_living_tables(soup)
            if not tables:
                logger.warning(f"No tables found for {city_name} Cost of Living")
                return []
            logger.info(f"Found {len(tables)} tables for {city_name} Cost of Living")
            return tables
        except Exception as e:
            logger.error(f"Error scraping Cost of Living for {city_name}: {e}")
            return []
    def _extract_cost_of_living_tables(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        tables = []
        tables_html = soup.find_all("table", class_="data_wide_table")
        for idx, table in enumerate(tables_html):
            try:
                df = pd.read_html(str(table))[0]
                caption = None
                if table.caption and table.caption.text.strip():
                    caption = table.caption.text.strip()
                else:
                    prev = table.find_previous(["h2", "h3"])
                    if prev and prev.text.strip():
                        caption = prev.text.strip()
                if not caption:
                    caption = f"Table{idx+1}"
                safe_caption = re.sub(r'[\\/*?:\[\]]', '', caption)[:31]
                df['table_caption'] = safe_caption
                df['category'] = self.category_name
                df = self._clean_dataframe(df)
                tables.append(df)
                logger.debug(f"Extracted table '{safe_caption}' with {len(df)} rows")
            except Exception as e:
                logger.error(f"Erreur lors de la conversion d'une table cost_of_living: {e}")
        return tables
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.dropna(how='all')
            data_columns = [col for col in df.columns if col not in ['table_caption', 'category']]
            if data_columns:
                df = df.dropna(subset=data_columns, how='all')
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            return df
        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {e}")
            return df
    def _is_blocked_page(self, soup: BeautifulSoup) -> bool:
        page_text = soup.get_text().lower()
        blocking_indicators = [
            'rate limit',
            'captcha',
            'blocked',
            'access denied',
            'too many requests',
            'please wait',
            'service temporarily unavailable'
        ]
        return any(indicator in page_text for indicator in blocking_indicators) 