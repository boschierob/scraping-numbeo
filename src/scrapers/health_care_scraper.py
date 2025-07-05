import pandas as pd
import logging
from typing import List
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class HealthCareScraper(BaseScraper):
    """Scraper for Health Care category"""
    def __init__(self):
        super().__init__()
        self.category_name = "health_care"

    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        logger.info(f"Scraping Health Care for {city_name}, {country_name}")
        try:
            soup = self.get_page(url)
            if not soup:
                logger.error(f"Failed to fetch page: {url}")
                return []
            if self._is_blocked_page(soup):
                logger.warning(f"Page appears to be blocked: {url}")
                self.save_debug_html(soup, f"blocked_{city_name}_{self.category_name}")
                return []
            tables = []
            # Table 1: indices globaux
            indices_table = soup.find("table", class_="table_indices")
            if indices_table:
                df = self._extract_indices_table(indices_table)
                if not df.empty:
                    tables.append(df)
            # Table 2: data_wide_table
            wide_tables = soup.find_all("table", class_="data_wide_table")
            for idx, wide_table in enumerate(wide_tables):
                df = self._extract_data_wide_table(wide_table, idx)
                if not df.empty:
                    tables.append(df)
            if not tables:
                logger.warning(f"No tables found for {city_name} Health Care")
            else:
                logger.info(f"Found {len(tables)} tables for {city_name} Health Care")
            return tables
        except Exception as e:
            logger.error(f"Error scraping Health Care for {city_name}: {e}")
            return []

    def _extract_indices_table(self, table) -> pd.DataFrame:
        """Extract key/value pairs from table_indices"""
        try:
            keys = []
            values = []
            trs = table.find_all("tr")
            for row in trs:
                tds = row.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(strip=True)
                    value = tds[1].get_text(strip=True)
                    if key and value:
                        keys.append(key)
                        values.append(value)
            if keys and values:
                df = pd.DataFrame({
                    "Category": keys,
                    "Value": values
                })
                df['table_caption'] = "Health Care Indices"
                df['category'] = self.category_name
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting indices table: {e}")
            return pd.DataFrame()

    def _extract_data_wide_table(self, table, idx: int) -> pd.DataFrame:
        """Extract key/value pairs from data_wide_table tables (key=columnWithName, value=indexValueTd) - gère tbody absent"""
        try:
            keys = []
            values = []
            tbody = table.find("tbody")
            if tbody:
                trs = tbody.find_all("tr")
            else:
                trs = table.find_all("tr")
            for row in trs:
                key = ""
                value = ""
                for td in row.find_all("td"):
                    td_classes = td.get("class", [])
                    if "columnWithName" in td_classes:
                        key = td.get_text(strip=True)
                    if "indexValueTd" in td_classes:
                        value = td.get_text(strip=True)
                if key and value:
                    keys.append(key)
                    values.append(value)
            if keys and values:
                df = pd.DataFrame({
                    "Category": keys,
                    "Value": values
                })
                df['table_caption'] = f"Health Care Details Table {idx+1}"
                df['category'] = self.category_name
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting data_wide_table: {e}")
            return pd.DataFrame()

    def _is_blocked_page(self, soup: BeautifulSoup) -> bool:
        """Check if the soup contains blocking indicators (compatibilité)"""
        page_text = soup.get_text().lower()
        blocking_indicators = [
            'rate limit',
            'captcha',
            'blocked',
            'access denied',
            'too many requests',
            'please wait'
        ]
        return any(indicator in page_text for indicator in blocking_indicators) 