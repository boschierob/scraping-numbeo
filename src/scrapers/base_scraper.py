"""
Base scraper class with common functionality
"""
import requests
import time
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
from pathlib import Path
from ..config.settings import REQUEST_DELAY, MAX_RETRIES, TIMEOUT, USER_AGENT

logger = logging.getLogger(__name__)

class BaseScraper:
    """Base class for all scrapers with common functionality"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.request_count = 0
        
    def get_page(self, url: str, retries: int = MAX_RETRIES) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
        Returns:
            BeautifulSoup object or None if failed
        """
        for attempt in range(retries):
            try:
                logger.debug(f"Fetching {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()
                
                # Check for rate limiting or blocking
                if self._is_blocked(response):
                    logger.warning(f"Page appears to be blocked: {url}")
                    if attempt < retries - 1:
                        wait_time = (attempt + 1) * REQUEST_DELAY * 2
                        logger.info(f"Waiting {wait_time}s before retry")
                        time.sleep(wait_time)
                        continue
                    return None
                
                soup = BeautifulSoup(response.content, 'html.parser')
                self.request_count += 1
                
                # Rate limiting
                if self.request_count % 5 == 0:
                    logger.debug(f"Rate limiting: waiting {REQUEST_DELAY}s")
                    time.sleep(REQUEST_DELAY)
                
                return soup
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {url}: {e}")
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * REQUEST_DELAY
                    logger.info(f"Waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                else:
                    return None
                    
        return None
    
    def _is_blocked(self, response: requests.Response) -> bool:
        """
        Check if the response indicates blocking or rate limiting
        
        Args:
            response: HTTP response object
            
        Returns:
            True if blocked, False otherwise
        """
        # Check for common blocking indicators
        blocking_indicators = [
            'rate limit',
            'captcha',
            'blocked',
            'access denied',
            'too many requests',
            'please wait'
        ]
        
        content_lower = response.text.lower()
        return any(indicator in content_lower for indicator in blocking_indicators)
    
    def save_debug_html(self, soup: BeautifulSoup, filename: str):
        """
        Save HTML content for debugging
        
        Args:
            soup: BeautifulSoup object
            filename: Filename to save as
        """
        try:
            debug_dir = Path("debug")
            debug_dir.mkdir(exist_ok=True)
            
            file_path = debug_dir / f"{filename}.html"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(str(soup))
            
            logger.info(f"Saved debug HTML: {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving debug HTML: {e}")
    
    def extract_table_data(self, table) -> List[Dict[str, Any]]:
        """
        Extract data from an HTML table
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            List of dictionaries representing table rows
        """
        try:
            rows = []
            headers = []
            
            # Extract headers
            header_row = table.find('thead')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            else:
                # Try to get headers from first row
                first_row = table.find('tr')
                if first_row:
                    headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]
            
            # If no headers found, generate default ones
            if not headers:
                first_row = table.find('tr')
                if first_row:
                    num_cols = len(first_row.find_all(['th', 'td']))
                    headers = [f"Column_{i+1}" for i in range(num_cols)]
            
            # Extract data rows
            data_rows = table.find_all('tr')[1:] if header_row else table.find_all('tr')
            
            for row in data_rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            value = cell.get_text(strip=True)
                            row_data[headers[i]] = value
                    rows.append(row_data)
            
            return rows
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            return []
    
    def get_table_caption(self, table) -> str:
        """
        Get table caption or nearby header
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            Caption or header text
        """
        try:
            # Try to get caption
            caption = table.find('caption')
            if caption:
                return caption.get_text(strip=True)
            
            # Look for nearby h3 headers
            prev_h3 = table.find_previous('h3')
            if prev_h3:
                return prev_h3.get_text(strip=True)
            
            # Look for nearby h2 headers
            prev_h2 = table.find_previous('h2')
            if prev_h2:
                return prev_h2.get_text(strip=True)
            
            return "Unknown Table"
            
        except Exception as e:
            logger.error(f"Error getting table caption: {e}")
            return "Unknown Table"
    
    def validate_table(self, table, min_rows: int = 3) -> bool:
        """
        Validate if a table has sufficient data
        
        Args:
            table: BeautifulSoup table element
            min_rows: Minimum number of rows required
            
        Returns:
            True if table is valid, False otherwise
        """
        try:
            rows = table.find_all('tr')
            return len(rows) >= min_rows
        except Exception:
            return False
    
    def cleanup(self):
        """Clean up resources"""
        if self.session:
            self.session.close() 