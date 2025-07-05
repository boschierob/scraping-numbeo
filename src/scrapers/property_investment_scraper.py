"""
Property Investment scraper for Numbeo
"""
import pandas as pd
import logging
from typing import List
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class PropertyInvestmentScraper(BaseScraper):
    """Scraper for Property Investment category"""
    def __init__(self):
        super().__init__()
        self.category_name = "property_investment"

    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        logger.info(f"Scraping Property Investment for {city_name}, {country_name}")
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
            # Table 2: toutes les tables contenant 'data_wide_table' dans leurs classes
            wide_tables = [t for t in soup.find_all("table") if t.has_attr("class") and any("data_wide_table" in c for c in t["class"])]
            for idx, wide_table in enumerate(wide_tables):
                df = self._extract_data_wide_table(wide_table, idx)
                if not df.empty:
                    tables.append(df)
            return tables
        except Exception as e:
            logger.error(f"Error scraping Property Investment for {city_name}: {e}")
            return []

    def _extract_indices_table(self, table) -> pd.DataFrame:
        """Extract key/value pairs from table_indices (structure adaptée)"""
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
                df['table_caption'] = "Property Investment Indices"
                df['category'] = self.category_name
                # Garder uniquement les colonnes pertinentes
                return df[["Category", "Value", "table_caption", "category"]]
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting indices table: {e}")
            return pd.DataFrame()

    def _extract_data_wide_table(self, table, idx: int) -> pd.DataFrame:
        """Extract key/price_value/price_min/price_max/section from data_wide_table tables (structure adaptée)"""
        try:
            rows = []
            section = None
            tbody = table.find("tbody")
            trs = tbody.find_all("tr") if tbody else table.find_all("tr")
            for row in trs:
                # Si <th> avec .category_title, c'est une nouvelle section
                th = row.find("th", class_="highlighted_th")
                if th:
                    div = th.find("div", class_="category_title")
                    if div:
                        section = div.get_text(strip=True)
                    continue  # ne pas traiter les lignes de titre comme données
                tds = row.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(strip=True)
                    price_value = None
                    price_min = None
                    price_max = None
                    for td in tds[1:]:
                        td_classes = td.get("class", [])
                        if "priceValue" in td_classes:
                            price_value = td.get_text(strip=True)
                        span_min = td.find("span", class_="barTextLeft")
                        if span_min:
                            price_min = span_min.get_text(strip=True)
                        span_max = td.find("span", class_="barTextRight")
                        if span_max:
                            price_max = span_max.get_text(strip=True)
                    rows.append({
                        "section": section,
                        "sub_section": key,
                        "Value": price_value,
                        "price_min": price_min,
                        "price_max": price_max
                    })
            if rows:
                df = pd.DataFrame(rows)
                df['table_caption'] = f"Property Investment Details Table {idx+1}"
                df['category'] = self.category_name
                # Garder uniquement les colonnes pertinentes
                return df[["section", "sub_section", "Value", "price_min", "price_max", "table_caption", "category"]]
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