"""
Traffic scraper for Numbeo
"""
import pandas as pd
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from ..config.settings import TABLE_SELECTORS, MIN_TABLE_ROWS

logger = logging.getLogger(__name__)

class TrafficScraper(BaseScraper):
    """Scraper for Traffic category"""
    
    def __init__(self):
        super().__init__()
        self.category_name = "traffic"
        self.table_selectors = TABLE_SELECTORS.get("traffic", ["table_builder_with_value_explanation", "data_wide_table"])
        
        # Specific H3 titles to look for
        self.h3_titles = [
            "Main Means of Transportation to Work or School",
            "Overall Average One-Way Commute Time and Distance to Work or School",
            "Average when primarily using Walking",
            "Average when primarily using Car",
            "Average when primarily using Bicycle",
            "Average when primarily using Bus/Trolleybus",
            "Average when primarily using Tram/Streetcar",
            "Average when primarily using Train/Metro"
        ]
    
    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        """
        Scrape Traffic data from the given URL
        
        Args:
            url: URL of the traffic page
            city_name: Name of the city
            country_name: Name of the country
            
        Returns:
            List of DataFrames containing scraped data
        """
        logger.info(f"Scraping Traffic for {city_name}, {country_name}")
        
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
            tables = self._extract_traffic_tables(soup)
            
            if not tables:
                logger.warning(f"No tables found for {city_name} Traffic")
                return []
            
            logger.info(f"Found {len(tables)} tables for {city_name} Traffic")
            return tables
            
        except Exception as e:
            logger.error(f"Error scraping Traffic for {city_name}: {e}")
            return []
    
    def _extract_traffic_tables(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        """
        Extract all relevant tables from the Traffic page
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of DataFrames
        """
        tables = []
        
        # 1. Find indices table
        indices_table = soup.find("table", class_="table_indices")
        if indices_table and self.validate_table(indices_table):
            df = self._extract_table_dataframe(indices_table, "Traffic Indices")
            if not df.empty:
                tables.append(df)
                logger.debug(f"Extracted traffic indices table with {len(df)} rows")
        
        # 2. Find tables following specific H3 headers
        for h3 in soup.find_all("h3"):
            if h3.text.strip() in self.h3_titles:
                next_table = h3.find_next_sibling("table")
                if next_table and self.validate_table(next_table):
                    caption = h3.text.strip()
                    df = self._extract_table_dataframe(next_table, caption)
                    if not df.empty:
                        tables.append(df)
                        logger.debug(f"Extracted table '{caption}' with {len(df)} rows")
        
        # 3. Find tables with specific classes
        for selector in self.table_selectors:
            found_tables = soup.find_all("table", class_=selector)
            for idx, table in enumerate(found_tables):
                if self.validate_table(table):
                    caption = self._get_table_caption(table, idx)
                    df = self._extract_table_dataframe(table, caption)
                    if not df.empty:
                        tables.append(df)
                        logger.debug(f"Extracted table '{caption}' with {len(df)} rows")
        
        return tables
    
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
        return f"TrafficTable{idx+1}"
    
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