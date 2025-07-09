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
import json
import glob
import psycopg2
from automate_supabase_json import collect_city_data, get_postgres_conn, create_table_if_needed, insert_city_json
from datetime import datetime

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

def scrape_from_slug(slug, country_name="DirectSlug", region="", label="DirectSlug"):
    setup_logging()
    logger = logging.getLogger(__name__)
    print(f"\n\033[1;35m=== Début scraping du slug : {slug} ===\033[0m")
    logger.info(f"=== Début scraping du slug : {slug} (country: {country_name}, region: {region}) ===")

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

    output_folder = make_city_output_folder(slug, region, country_name if country_name else label)
    file_saver = FileSaver(output_folder=output_folder)
    scraper_factory = ScraperFactory()

    slug_success = True
    for cat in categories:
        cat_url = f"https://www.numbeo.com/{cat}/in/{slug}"
        print(f"\033[1;36m➡️  Scraping catégorie : {cat} | URL: {cat_url}\033[0m")
        logger.info(f"Scraping category '{cat}' at URL: {cat_url}")
        scraper = scraper_factory.get_scraper(cat.replace('-', '_'))
        try:
            tables = scraper.scrape_category(cat_url, city_name=slug, country_name=country_name if country_name else label)
            if tables:
                file_saver.save_category_data(slug, country_name if country_name else label, cat, tables)
                print(f"\033[1;32m✅ Succès : données sauvegardées pour {cat_url}\033[0m")
                logger.info(f"Scraping and saving completed for {cat_url}.")
            else:
                print(f"\033[1;33m⚠️  Avertissement : aucune table trouvée pour {cat_url}\033[0m")
                logger.warning(f"No tables found at {cat_url}.")
        except Exception as e:
            print(f"\033[1;31m❌ Erreur lors du scraping de {cat_url} : {e}\033[0m")
            logger.error(f"Error scraping {cat_url}: {e}")
            slug_success = False

    if slug_success:
        print(f"\033[1;32m=== Fin scraping du slug : {slug} (succès) ===\033[0m\n")
        logger.info(f"=== Fin scraping du slug : {slug} (succès) ===")
    else:
        print(f"\033[1;31m=== Fin scraping du slug : {slug} (avec erreurs) ===\033[0m\n")
        logger.warning(f"=== Fin scraping du slug : {slug} (avec erreurs) ===")


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

    if category:
        # Scraper uniquement la catégorie demandée
        output_folder = make_city_output_folder(slug, '', 'DirectURL')
        file_saver = FileSaver(output_folder=output_folder)
        scraper_factory = ScraperFactory()
        cat_url = f"https://www.numbeo.com/{category}/in/{slug}"
        print(f"\033[1;36m➡️  Scraping category:\033[0m {category} | URL: {cat_url}")
        scraper = scraper_factory.get_scraper(category.replace('-', '_'))
        try:
            tables = scraper.scrape_category(cat_url, city_name=slug, country_name="DirectURL")
            if tables:
                file_saver.save_category_data(slug, "DirectURL", category, tables)
                print(f"\033[1;32m✅ Success:\033[0m Data saved for {cat_url}")
            else:
                print(f"\033[1;33m⚠️  Warning:\033[0m No tables found at {cat_url}")
        except Exception as e:
            print(f"\033[1;31m❌ Error:\033[0m scraping {cat_url}: {e}")
    else:
        # Scraper toutes les catégories via la fonction factorisée
        scrape_from_slug(slug, country_name="DirectURL", region="", label="DirectURL")

def automate_supabase_for_all_outputs(folders=None):
    output_root = Path('output')
    if folders is None:
        folders = [f for f in output_root.iterdir() if f.is_dir()]
    for city_folder in folders:
        meta_path = city_folder / "meta.json"
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            city = meta.get("city")
            region = meta.get("region")
            country = meta.get("country")
            datestamp = meta.get("datestamp")
            if not city or not country:
                print(f"meta.json incomplet dans {city_folder}, dossier ignoré.")
                continue
        else:
            # Fallback: parser le nom du dossier (ancienne méthode)
            print(f"[WARN] Pas de meta.json dans {city_folder}, tentative de parsing du nom de dossier.")
            folder_name = city_folder.name
            import re
            m = re.match(r'(.+)-(\d{8}_\d{6})$', folder_name)
            if not m:
                print(f"Nom de dossier non conforme: {folder_name}, dossier ignoré.")
                continue
            base, timestamp = m.groups()
            parts = base.split('-')
            if len(parts) >= 4:
                country = '-'.join(parts[-2:])
                region = parts[-3]
                city = '-'.join(parts[:-3])
            elif len(parts) == 3:
                country = '-'.join(parts[-2:])
                region = None
                city = parts[0]
            elif len(parts) == 2:
                country = parts[-1]
                region = None
                city = parts[0]
            else:
                print(f"Impossible de parser le nom du dossier: {folder_name}, dossier ignoré.")
                continue
            datestamp = timestamp
        try:
            city_json = collect_city_data(str(city_folder), city, country, datestamp, region)
            with open(os.path.join(str(city_folder), "city_data.json"), "w", encoding="utf-8") as f:
                json.dump(city_json, f, ensure_ascii=False, indent=2)
            conn = get_postgres_conn()
            create_table_if_needed(conn)
            insert_city_json(conn, city_json)
            conn.close()
            print(f"✅ Supabase: données insérées pour {city}, {country}")
        except Exception as e:
            print(f"❌ Erreur Supabase pour {city}, {country} : {e}")

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

                # --- AUTOMATISATION SUPABASE ---
                try:
                    # Recréer le même dossier d'output que pour le scraping
                    output_folder = make_city_output_folder(city_name, region, country_name)
                    datestamp = datetime.now().isoformat()
                    city_json = collect_city_data(str(output_folder), city_name, country_name, datestamp, region)
                    # Sauvegarde locale pour debug
                    with open(os.path.join(str(output_folder), "city_data.json"), "w", encoding="utf-8") as f:
                        json.dump(city_json, f, ensure_ascii=False, indent=2)
                        print(f"💾 JSON saved to {os.path.join(str(output_folder), 'city_data.json')}")
                    # Insertion dans Supabase
                    conn = get_postgres_conn()
                    create_table_if_needed(conn)
                    insert_city_json(conn, city_json)
                    conn.close()
                    print(f"✅ Supabase: données insérées pour {city_name}, {country_name}")
                except Exception as e:
                    logger.error(f"Erreur lors de l'automatisation Supabase pour {city_name}, {country_name} : {e}")
                    print(f"\033[1;31m❌ Erreur Supabase pour {city_name}, {country_name} : {e}\033[0m")

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

        if report_file:
            logger.info(f"Scraping completed. Report saved to: {report_file}")
        
        # --- AUTOMATISATION SUPABASE POUR TOUS LES DOSSIERS ---
        # This part is now handled by the new automate_supabase_for_all_outputs function
        # which can take a list of folders to process.
        # For the interactive mode, we'll call it with the newly created folders.
        # In the case of the CSV mode, it will process all folders in 'output'.
        # In the case of the URL mode, it will process only the newly created folders.
        # The original call to automate_supabase_for_all_outputs() is removed.
        
        # Check if scraping was successful
        if stats_tracker.is_scraping_successful():
            logger.info("Scraping session was successful")
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
        created_folders = []
        for url in args.urls:
            # Avant scraping, lister les dossiers existants
            before = set(f.name for f in Path('output').iterdir() if f.is_dir())
            scrape_from_url(url, args.category)
            after = set(f.name for f in Path('output').iterdir() if f.is_dir())
            new_folders = after - before
            created_folders.extend([Path('output')/f for f in new_folders])
        if created_folders:
            automate_supabase_for_all_outputs(created_folders)
        sys.exit(0)
    else:
        # Mode interactif
        print("Que voulez-vous faire ?")
        print("1. Scraper toutes les villes du fichier CSV")
        print("2. Scraper une ou plusieurs URLs Numbeo")
        print("3. Entrer une ou plusieurs localités (slugs Numbeo)")
        choix = input("Votre choix [1/2/3] : ").strip()
        if choix == "1":
            before = set(f.name for f in Path('output').iterdir() if f.is_dir())
            success = main()
            after = set(f.name for f in Path('output').iterdir() if f.is_dir())
            created_folders = [Path('output')/f for f in (after - before)]
            if created_folders:
                automate_supabase_for_all_outputs(created_folders)
            sys.exit(0)
        elif choix == "2":
            print("Entrez une ou plusieurs URLs Numbeo, séparées par un espace :")
            line = input()
            urls = [u for u in line.strip().split() if u]
            if not urls:
                print("Aucune URL entrée. Fin du programme.")
                sys.exit(1)
            created_folders = []
            for url in urls:
                before = set(f.name for f in Path('output').iterdir() if f.is_dir())
                scrape_from_url(url)
                after = set(f.name for f in Path('output').iterdir() if f.is_dir())
                new_folders = after - before
                created_folders.extend([Path('output')/f for f in new_folders])
            if created_folders:
                automate_supabase_for_all_outputs(created_folders)
            sys.exit(0)
        elif choix == "3":
            print("Attention : Veuillez entrer la localité exactement comme elle apparaît dans le slug Numbeo (ex : Paris, Lyon, New-York, etc.)")
            print("Entrez une ou plusieurs localités (slugs Numbeo), séparées par un espace :")
            line = input()
            slugs = [s for s in line.strip().split() if s]
            if not slugs:
                print("Aucun slug entré. Fin du programme.")
                sys.exit(1)
            print(f"\n\033[1;34m--- Début du scraping pour {len(slugs)} slug(s) ---\033[0m")
            created_folders = []
            for slug in slugs:
                before = set(f.name for f in Path('output').iterdir() if f.is_dir())
                scrape_from_slug(slug)
                after = set(f.name for f in Path('output').iterdir() if f.is_dir())
                new_folders = after - before
                created_folders.extend([Path('output')/f for f in new_folders])
            print(f"\n\033[1;34m--- Fin du scraping pour tous les slugs ---\033[0m")
            if created_folders:
                automate_supabase_for_all_outputs(created_folders)
            sys.exit(0)
        else:
            print("Choix invalide. Fin du programme.")
            sys.exit(1) 