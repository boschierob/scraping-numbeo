"""
Quality of Life scraper for Numbeo
"""
import pandas as pd
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from ..config.settings import TABLE_SELECTORS, MIN_TABLE_ROWS

logger = logging.getLogger(__name__)

class QualityOfLifeScraper(BaseScraper):
    """Scraper for Quality of Life category"""
    
    def __init__(self):
        super().__init__()
        self.category_name = "quality_of_life"
        self.table_selector = TABLE_SELECTORS.get("default", "table_builder_with_value_explanation")
    
    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        """
        Scrape Quality of Life data from the given URL
        
        Args:
            url: URL of the quality of life page
            city_name: Name of the city
            country_name: Name of the country
            
        Returns:
            List of DataFrames containing scraped data
        """
        logger.info(f"Scraping Quality of Life for {city_name}, {country_name}")
        
        try:
            # Fetch the page
            soup = self.get_page(url)
            if not soup:
                logger.error(f"Failed to fetch page: {url}")
                return []
            
            # Check for blocking
            if self._is_blocked_page(soup):
                logger.warning(f"Page appears to be blocked: {url}")
                self.save_debug_html(soup, f"blocked_{city_name}_{self.category_name}")
                return []
            
            # Extract tables
            tables = self._extract_quality_of_life_tables(soup)
            
            if not tables:
                logger.warning(f"No tables found for {city_name} Quality of Life")
                return []
            
            logger.info(f"Found {len(tables)} tables for {city_name} Quality of Life")
            return tables
            
        except Exception as e:
            logger.error(f"Error scraping Quality of Life for {city_name}: {e}")
            return []
    
    def _extract_quality_of_life_tables(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        """
        Extract all relevant tables from the Quality of Life page
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of DataFrames
        """
        tables = []
        
        # Find main quality of life table (the summary table with discreet_link)
        main_table = self._find_main_summary_table(soup)
        if main_table:
            df = self._extract_main_table_dataframe(main_table, "Quality of Life Indices")
            if not df.empty:
                tables.append(df)
                logger.debug(f"Extracted main Quality of Life table with {len(df)} rows")
        
        # Find tables with the specific class
        selected_tables = soup.find_all("table", class_=self.table_selector)
        for idx, table in enumerate(selected_tables):
            if table != main_table and self.validate_table(table):
                caption = self._get_table_caption(table, idx)
                df = self._extract_table_dataframe(table, caption)
                if not df.empty:
                    tables.append(df)
                    logger.debug(f"Extracted table '{caption}' with {len(df)} rows")
        
        return tables
    
    def _find_main_summary_table(self, soup: BeautifulSoup):
        """
        Find the main quality of life summary table (with discreet_link)
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element or None
        """
        for table in soup.find_all("table"):
            # Exclude tables inside <aside>
            if table.find_parent("aside") is None:
                # Look for table with discreet_link in first td
                rows = table.find_all("tr")
                for row in rows:
                    first_td = row.find("td")
                    if first_td:
                        discreet_link = first_td.find("a", class_="discreet_link")
                        if discreet_link:
                            logger.debug(f"Found main table with discreet_link: {discreet_link.text.strip()}")
                            return table
        return None
    
    def _get_table_caption(self, table, idx: int) -> str:
        """
        Get table caption or generate a fallback name
        
        Args:
            table: BeautifulSoup table element
            idx: Table index for fallback naming
            
        Returns:
            Caption or generated name
        """
        # Try to get caption
        if table.caption and table.caption.text.strip():
            return table.caption.text.strip()
        
        # Try to find a preceding <h2> or <h3> as a title
        prev = table.find_previous(["h2", "h3"])
        if prev and prev.text.strip():
            return prev.text.strip()
        
        # Fallback to Table1, Table2, ...
        return f"Table{idx+1}"
    
    def _extract_table_dataframe(self, table, caption: str) -> pd.DataFrame:
        """
        Extract data from a table and convert to DataFrame
        
        Args:
            table: BeautifulSoup table element
            caption: Table caption or name
            
        Returns:
            DataFrame with table data
        """
        try:
            # Use pandas read_html for better table parsing
            df = pd.read_html(str(table))[0]
            
            # Add metadata columns
            df['table_caption'] = caption
            df['category'] = self.category_name
            
            # Clean up the data
            df = self._clean_dataframe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and format the DataFrame
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        try:
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Remove rows where all columns except metadata are empty
            data_columns = [col for col in df.columns if col not in ['table_caption', 'category']]
            if data_columns:
                df = df.dropna(subset=data_columns, how='all')
            
            # Strip whitespace from string columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {e}")
            return df
    
    def _is_blocked_page(self, soup: BeautifulSoup) -> bool:
        """
        Check if the page is blocked or shows an error
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            True if page is blocked
        """
        # Check for common blocking indicators
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
    
    def _extract_main_table_dataframe(self, table, caption: str) -> pd.DataFrame:
        """
        Extract data from the main quality of life table with discreet_link structure
        
        Args:
            table: BeautifulSoup table element
            caption: Table caption or name
            
        Returns:
            DataFrame with categories and values
        """
        try:
            categories = []
            values = []
            
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    # First td: get text from discreet_link
                    first_td = cells[0]
                    discreet_link = first_td.find("a", class_="discreet_link")
                    if discreet_link:
                        category = discreet_link.text.strip()
                        # Second td: get the value
                        value = cells[1].get_text(strip=True)
                        
                        if category and value:
                            # Replace the "ƒ" symbol with "Quality of Life Index"
                            if category == "ƒ":
                                category = "Quality of Life Index"
                            
                            categories.append(category)
                            values.append(value)
                            logger.debug(f"Extracted: {category} = {value}")
            
            # Create DataFrame
            if categories and values:
                df = pd.DataFrame({
                    'Categories': categories,
                    'Values': values
                })
                
                # Add metadata columns
                df['table_caption'] = caption
                df['category'] = self.category_name
                
                return df
            else:
                logger.warning("No categories/values found in main table")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error extracting main table data: {e}")
            return pd.DataFrame()