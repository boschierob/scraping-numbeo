#!/usr/bin/env python3
"""
Main entry point for Numbeo scraping application
"""
import logging
import sys
from pathlib import Path
import argparse
from urllib.parse import urlparse
import os
import subprocess

from dotenv import load_dotenv
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config.settings import ensure_directories, LOG_LEVEL, LOG_FORMAT, LOG_FILE
from src.data.city_loader import CityLoader
from src.utils.url_builder import URLBuilder
from src.utils.file_saver import FileSaver, make_city_output_folder
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

def parse_args():
    parser = argparse.ArgumentParser(description="Numbeo Scraper")
    parser.add_argument('--urls', nargs='+', type=str, help="Scrape one or more city/category URLs (separated by space)")
    parser.add_argument('--category', type=str, help="Category to use with the URLs (optional)")
    return parser.parse_args()

def extract_slug(url):
    # Extrait le slug après /in/ dans l'URL
    path = urlparse(url).path
    if '/in/' in path:
        return path.split('/in/')[1]
    return None

def scrape_from_url(url, category=None):
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f"Scraping direct URL: {url} (category: {category})")
    print(f"\033[1;34m🔎 Scraping direct URL:\033[0m {url} (category: {category})")

    slug = extract_slug(url)
    if not slug:
        logger.error(f"Could not extract slug from URL: {url}")
        print(f"\033[1;31m❌ Could not extract slug from URL:\033[0m {url}")
        return

    # Liste des catégories à scraper
    categories = [
        "quality-of-life",
        "crime",
        "cost-of-living",
        "health-care",
        "climate",
        "property-investment",
        "traffic",
        "pollution"
    ]

    # Si une catégorie est précisée, ne scraper que celle-ci
    if category:
        categories = [category]

    output_folder = make_city_output_folder(slug, '', 'DirectURL')
    file_saver = FileSaver(output_folder=output_folder)

    scraper_factory = ScraperFactory()

    for cat in categories:
        cat_url = f"https://www.numbeo.com/{cat}/in/{slug}"
        logger.info(f"Scraping category '{cat}' at URL: {cat_url}")
        print(f"\033[1;36m➡️  Scraping category:\033[0m {cat} | URL: {cat_url}")
        scraper = scraper_factory.get_scraper(cat.replace('-', '_'))
        try:
            tables = scraper.scrape_category(cat_url, city_name=slug, country_name="DirectURL")
            if tables:
                file_saver.save_category_data(slug, "DirectURL", cat, tables)
                logger.info(f"Scraping and saving completed for {cat_url}.")
                print(f"\033[1;32m✅ Success:\033[0m Data saved for {cat_url}")
            else:
                logger.warning(f"No tables found at {cat_url}.")
                print(f"\033[1;33m⚠️  Warning:\033[0m No tables found at {cat_url}")
        except Exception as e:
            logger.error(f"Error scraping {cat_url}: {e}")
            print(f"\033[1;31m❌ Error:\033[0m scraping {cat_url}: {e}")

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
        scraper_factory = ScraperFactory()
        stats_tracker = StatsTracker()
        
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
            city_name = (city.get('city', '') or '').strip() or 'UnknownCity'
            region = (city.get('region', '') or '').strip()
            country_name = (city.get('country', '') or '').strip() or 'UnknownCountry'
            city_url = city.get('url', '')
            
            print(f"\n\033[1;35m--- Début scraping ville : {city_name}, {country_name} ---\033[0m")
            logger.info(f"--- Début scraping ville : {city_name}, {country_name} ---")

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
                    print(f"\033[1;36m  → Début scraping catégorie : {category} pour {city_name}\033[0m")
                    logger.info(f"  → Début scraping catégorie : {category} pour {city_name}")
                    try:
                        logger.debug(f"    URL: {category_url}")
                        # Utiliser le ScraperFactory pour obtenir le bon scraper
                        scraper = scraper_factory.get_scraper(category)
                        output_folder = make_city_output_folder(city_name, region, country_name)
                        file_saver = FileSaver(output_folder=output_folder)
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
                        print(f"\033[1;32m  ✅ Fin scraping catégorie : {category} pour {city_name}\033[0m")
                        logger.info(f"  ✅ Fin scraping catégorie : {category} pour {city_name}")
                    except Exception as e:
                        logger.error(f"    Error scraping {category}: {e}")
                        stats_tracker.record_error("scraping_error", str(e), city_name, category)
                        stats_tracker.record_category_result(
                            city_name, country_name, category, 
                            success=False, tables_found=0, tables_successful=0, files_created=0
                        )
                        city_success = False
                        print(f"\033[1;31m  ❌ Erreur scraping catégorie : {category} pour {city_name} : {e}\033[0m")
                        logger.info(f"  ❌ Erreur scraping catégorie : {category} pour {city_name} : {e}")
                
                # Après la boucle des catégories pour la ville
                print(f"  🟢 Fin de toutes les catégories pour {city_name}, passage à la fin de la ville.")
                logger.info(f"  🟢 Fin de toutes les catégories pour {city_name}, passage à la fin de la ville.")
                stats_tracker.record_city_end(city_name, country_name, city_success)
                print(f"\033[1;35m--- Fin scraping ville : {city_name}, {country_name} ---\033[0m\n")
                logger.info(f"--- Fin scraping ville : {city_name}, {country_name} ---")
            
            except Exception as e:
                logger.error(f"Error processing city {city_name}: {e}")
                stats_tracker.record_error("city_error", str(e), city_name)
                stats_tracker.record_city_end(city_name, country_name, False)
                print(f"\033[1;31m❌ Erreur scraping ville : {city_name}, {country_name} : {e}\033[0m")
                logger.info(f"❌ Erreur scraping ville : {city_name}, {country_name} : {e}")
        
        # End tracking and generate report
        stats_tracker.end_scraping()
        report_file = stats_tracker.generate_report(file_saver.get_output_folder())

        # Appel automatique de l'import MySQL pour tous les dossiers d'output générés
        output_root = Path('output')
        for subdir in output_root.iterdir():
            if subdir.is_dir():
                try:
                    subprocess.run([
                        sys.executable, 'import_to_mysql.py', str(subdir)
                    ], check=True)
                except Exception as e:
                    logger.error(f"Erreur lors de l'import MySQL automatique pour {subdir} : {e}")

        print("[DEBUG] Bloc upload Google Drive va être évalué.")
        print(f"[DEBUG] report_file = {report_file}")
        
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
        if 'scraper_factory' in locals():
            scraper_factory.cleanup()

    # Après la boucle des villes
    print("✅ Fin du scraping de toutes les villes.")
    logger.info("Fin du scraping de toutes les villes.")

if __name__ == "__main__":
    args = parse_args()
    if args.urls:
        for url in args.urls:
            scrape_from_url(url, args.category)
        sys.exit(0)
    else:
        success = main()
        sys.exit(0 if success else 1) 