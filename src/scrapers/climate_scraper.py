"""
Climate scraper for Numbeo
"""
import pandas as pd
import logging
from typing import List
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
import re

logger = logging.getLogger(__name__)

class ClimateScraper(BaseScraper):
    """Scraper for Climate category (structure tabulaire par h2)"""
    def __init__(self):
        super().__init__()
        self.category_name = "climate"

    def scrape_category(self, url: str, city_name: str, country_name: str) -> List[pd.DataFrame]:
        logger.info(f"Scraping Climate for {city_name}, {country_name}")
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
            # Associer chaque table ou <p> à son h2 précédent
            for h2 in soup.find_all("h2"):
                next_elem = h2.find_next_sibling()
                # Si c'est un <p> (et pas un <table>), extraire le texte et parser les mois
                if next_elem and next_elem.name == "p":
                    p_text = next_elem.get_text(strip=True)
                    months = re.findall(r'(January|February|March|April|May|June|July|August|September|October|November|December)', p_text)
                    if months:
                        df = pd.DataFrame({"Best Months": months})
                        df['table_caption'] = h2.get_text(strip=True)
                        df['category'] = self.category_name
                        df['data_type'] = 'list'
                        tables.append(df)
                    # Ne pas extraire la table qui suit ce <h2> si un <p> est présent
                    continue
                # Sinon, extraire la table comme avant
                table = h2.find_next("table")
                if not table:
                    continue
                table_caption = h2.get_text(strip=True)
                trs = table.find_all("tr")
                if not trs:
                    continue
                header_tds = trs[0].find_all("td")
                keys = [td.get_text(strip=True) for td in header_tds]
                data = []
                for row in trs[1:]:
                    tds = row.find_all("td")
                    row_vals = []
                    for td in tds:
                        div = td.find("div")
                        if div:
                            row_vals.append(div.get_text(strip=True))
                        else:
                            row_vals.append(td.get_text(strip=True))
                    while len(row_vals) < len(keys):
                        row_vals.append(None)
                    data.append(row_vals)
                if data:
                    df = pd.DataFrame(data, columns=keys)
                    df['table_caption'] = table_caption
                    df['category'] = self.category_name
                    df['data_type'] = 'table'
                    tables.append(df)
            if not tables:
                logger.warning(f"No tables found for {city_name} Climate. Saving debug HTML.")
                self.save_debug_html(soup, f"no_tables_{city_name}_{self.category_name}")
            return tables
        except Exception as e:
            logger.error(f"Error scraping Climate for {city_name}: {e}")
            return []

    def _is_blocked_page(self, soup: BeautifulSoup) -> bool:
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