#!/usr/bin/env python3
"""
Main entry point for Numbeo scraping application
"""
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config.settings import ensure_directories, LOG_LEVEL, LOG_FORMAT, LOG_FILE
from src.data.city_loader import CityLoader
from src.utils.url_builder import URLBuilder
from src.utils.file_saver import FileSaver
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.scraper_factory import ScraperFactory
from src.monitoring.stats_tracker import StatsTracker

def setup_logging():
    """Setup logging configuration"""
    ensure_directories()
    
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    """Main scraping function"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Numbeo scraping application")
    
    try:
        # Initialize components
        city_loader = CityLoader()
        url_builder = URLBuilder()
        file_saver = FileSaver()
        base_scraper = BaseScraper()
        stats_tracker = StatsTracker()
        scraper_factory = ScraperFactory()
        
        # Start tracking
        stats_tracker.start_scraping()
        
        # Load cities
        cities = city_loader.load_cities()
        if not cities:
            logger.error("No cities loaded. Exiting.")
            return False
        
        logger.info(f"Loaded {len(cities)} cities")
        stats_tracker.stats['total_cities'] = len(cities)
        
        # Process each city
        for city in cities:
            city_name = city.get('city', 'Unknown')
            country_name = city.get('country', 'Unknown')
            city_url = city.get('url', '')
            
            if not city_url:
                logger.warning(f"No URL for city {city_name}, {country_name}")
                continue
            
            logger.info(f"Processing city: {city_name}, {country_name}")
            stats_tracker.record_city_start(city_name, country_name)
            
            try:
                # Get all category URLs for this city
                category_urls = url_builder.get_all_category_urls(city_url)
                
                city_success = True
                for category, category_url in category_urls.items():
                    logger.info(f"  Scraping category: {category}")
                    try:
                        logger.debug(f"    URL: {category_url}")
                        # Utiliser le ScraperFactory pour obtenir le bon scraper
                        scraper = scraper_factory.get_scraper(category)
                        tables = scraper.scrape_category(category_url, city_name, country_name)
                        files_created = 0
                        tables_found = len(tables)
                        tables_successful = 0
                        if tables:
                            # Sauvegarder les tables extraites
                            saved_files = file_saver.save_category_data(city_name, country_name, category, tables)
                            files_created = len(saved_files)
                            tables_successful = len([df for df in tables if not df.empty])
                        stats_tracker.record_category_result(
                            city_name, country_name, category, 
                            success=(tables_successful > 0),
                            tables_found=tables_found,
                            tables_successful=tables_successful,
                            files_created=files_created
                        )
                    except Exception as e:
                        logger.error(f"    Error scraping {category}: {e}")
                        stats_tracker.record_error("scraping_error", str(e), city_name, category)
                        stats_tracker.record_category_result(
                            city_name, country_name, category, 
                            success=False, tables_found=0, tables_successful=0, files_created=0
                        )
                        city_success = False
                
                stats_tracker.record_city_end(city_name, country_name, city_success)
                
            except Exception as e:
                logger.error(f"Error processing city {city_name}: {e}")
                stats_tracker.record_error("city_error", str(e), city_name)
                stats_tracker.record_city_end(city_name, country_name, False)
        
        # End tracking and generate report
        stats_tracker.end_scraping()
        report_file = stats_tracker.generate_report(file_saver.get_output_folder())
        
        if report_file:
            logger.info(f"Scraping completed. Report saved to: {report_file}")
        
        # Check if scraping was successful
        if stats_tracker.is_scraping_successful():
            logger.info("Scraping session was successful")
            # TODO: Launch MySQL import script here
            return True
        else:
            logger.warning("Scraping session had low success rate")
            return False
            
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        return False
    
    finally:
        # Cleanup
        if 'base_scraper' in locals():
            base_scraper.cleanup()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 