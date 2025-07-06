import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import os
import pandas as pd
import random
from urllib.parse import quote
import re
import logging
import sys

# Configuration du logging (DEBUG)
logging.basicConfig(
    filename='logs/scraping.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.numbeo.com"
CITY_LIST_URL = f"{BASE_URL}/quality-of-life/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- GLOBAL: Dossier de sortie horodaté ---
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
TIMESTAMPED_OUTPUT_DIR = os.path.join(OUTPUT_DIR, TIMESTAMP)
os.makedirs(TIMESTAMPED_OUTPUT_DIR, exist_ok=True)

# --- MODULE: Chargement des villes ---
def load_cities():
    """Charge la liste des villes depuis une base de données ou un CSV de secours."""
    try:
        import sqlite3
        conn = sqlite3.connect('datas/cities.db')
        df = pd.read_sql_query("SELECT city, country FROM cities", conn)
        print("✅ Chargé depuis la base de données")
        return df
    except Exception as e:
        print("⚠️ Base de données non accessible, chargement du CSV de secours :", e)
        csv_path = os.path.join("datas", "cities.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            print("✅ Chargé depuis le CSV")
            return df
        else:
            raise FileNotFoundError("Aucune source de villes disponible (ni base, ni CSV)")

# --- MODULE: Construction de l'URL Numbeo pour une ville ---
def build_numbeo_url(city):
    city_part = city.replace(" ", "-")
    url = f"{BASE_URL}/quality-of-life/in/{quote(city_part)}"
    logger.debug(f"URL simple générée: {url}")
    return url

def build_numbeo_url_city_country(city, country):
    city_part = city.replace(" ", "-")
    country_part = country.replace(" ", "-")
    url = f"{BASE_URL}/quality-of-life/in/{quote(city_part)}-{quote(country_part)}"
    logger.debug(f"URL Ville-Pays générée: {url}")
    return url

def build_numbeo_url_city_state_country(city, state, country):
    city_part = city.replace(" ", "-")
    state_part = state.replace(" ", "-")
    country_part = country.replace(" ", "-")
    url = f"{BASE_URL}/quality-of-life/in/{quote(city_part)}-{quote(state_part)}-{quote(country_part)}"
    logger.debug(f"URL Ville-Etat-Pays générée: {url}")
    return url

# --- MODULE: Trouver la première URL valide pour une ville ---
def find_valid_city_url(city, country, state=None):
    """
    Logique robuste pour trouver l'URL Numbeo correcte :
    1. Tester /in/Ville
    2. Vérifier correspondance avec données CSV
    3. Adapter l'URL si nécessaire (pays, puis état)
    4. Arrêter avec erreur si rien ne correspond
    """
    
    # 1. Tester d'abord l'URL simple /in/Ville
    url_simple = build_numbeo_url(city)
    logger.debug(f"Test de l'URL simple: {url_simple}")
    
    try:
        resp = requests.get(url_simple, headers=HEADERS, timeout=10)
        logger.debug(f"Réponse HTTP {resp.status_code} pour {url_simple}")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            
            # Vérifier s'il y a des données (tableau ou info)
            if soup.find("table") or soup.find("h1"):
                # Vérifier la correspondance avec les données CSV
                match_result = _check_city_match(soup, city, country, state)
                
                if match_result == "EXACT_MATCH":
                    print(f"[DEBUG] ✅ Correspondance exacte trouvée avec {url_simple}")
                    return url_simple
                elif match_result == "CITY_MATCH_ONLY":
                    print(f"[DEBUG] ⚠️ Ville trouvée mais pays/état différent, on continue...")
                    # On continue vers les tests suivants
                else:
                    print(f"[DEBUG] ❌ Aucune correspondance avec {url_simple}")
            else:
                print(f"[DEBUG] ❌ Pas de données trouvées dans {url_simple}")
    except Exception as e:
        logger.debug(f"Exception lors de la requête {url_simple}: {e}")
    
    # 2. Si pas de correspondance exacte, tester /in/Ville-Pays
    url_country = build_numbeo_url_city_country(city, country)
    logger.debug(f"Test 2: {url_country}")
    
    try:
        resp = requests.get(url_country, headers=HEADERS, timeout=10)
        logger.debug(f"Réponse HTTP {resp.status_code} pour {url_country}")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            
            if soup.find("table") or soup.find("h1"):
                match_result = _check_city_match(soup, city, country, state)
                
                if match_result == "EXACT_MATCH":
                    print(f"[DEBUG] ✅ Correspondance exacte trouvée avec {url_country}")
                    return url_country
                elif match_result == "CITY_MATCH_ONLY":
                    print(f"[DEBUG] ⚠️ Ville trouvée mais pays/état différent, on continue...")
                else:
                    print(f"[DEBUG] ❌ Aucune correspondance avec {url_country}")
            else:
                print(f"[DEBUG] ❌ Pas de données trouvées dans {url_country}")
    except Exception as e:
        logger.debug(f"Erreur lors du test 2: {e}")
    
    # 3. Si state est renseigné, tester /in/Ville-Etat-Pays
    if state and isinstance(state, str) and state.strip():
        url_state = build_numbeo_url_city_state_country(city, state, country)
        logger.debug(f"Test 3: {url_state}")
        
        try:
            resp = requests.get(url_state, headers=HEADERS, timeout=10)
            logger.debug(f"Réponse HTTP {resp.status_code} pour {url_state}")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, "html.parser")
                
                if soup.find("table") or soup.find("h1"):
                    match_result = _check_city_match(soup, city, country, state)
                    
                    if match_result == "EXACT_MATCH":
                        print(f"[DEBUG] ✅ Correspondance exacte trouvée avec {url_state}")
                        return url_state
                    else:
                        print(f"[DEBUG] ❌ Aucune correspondance avec {url_state}")
                else:
                    print(f"[DEBUG] ❌ Pas de données trouvées dans {url_state}")
        except Exception as e:
            logger.debug(f"Erreur lors du test 3: {e}")
    
    # 4. Aucune URL valide trouvée
    print(f"[DEBUG] ❌ ERREUR: Aucune URL valide trouvée pour {city}, {country}{' ('+state+')' if state else ''}")
    return None

def _check_city_match(soup, city, country, state=None):
    """
    Vérifie la correspondance entre les données CSV et ce que Numbeo renvoie.
    Retourne: "EXACT_MATCH", "CITY_MATCH_ONLY", ou "NO_MATCH"
    """
    try:
        # Extraire les informations de la page Numbeo
        page_city, page_country, page_state = _extract_location_info(soup)
        
        if not page_city or not page_country:
            return "NO_MATCH"
        
        # Normaliser les noms pour la comparaison
        def normalize_name(name):
            """Normalise un nom en minuscules et remplace les tirets par des espaces"""
            return name.lower().replace('-', ' ')
        
        # Vérifier la correspondance de la ville
        if normalize_name(page_city) != normalize_name(city):
            print(f"[DEBUG] Ville ne correspond pas: CSV='{city}' vs Numbeo='{page_city}'")
            return "NO_MATCH"
        
        # Vérifier la correspondance du pays
        if normalize_name(page_country) != normalize_name(country):
            print(f"[DEBUG] Pays ne correspond pas: CSV='{country}' vs Numbeo='{page_country}'")
            return "CITY_MATCH_ONLY"
        
        # Vérifier la correspondance de l'état (si renseigné)
        if state and state.strip():
            if page_state and normalize_name(state) != normalize_name(page_state):
                print(f"[DEBUG] État ne correspond pas: CSV='{state}' vs Numbeo='{page_state}'")
                return "CITY_MATCH_ONLY"
        
        return "EXACT_MATCH"
        
    except Exception as e:
        print(f"[DEBUG] Erreur lors de la vérification de correspondance: {e}")
        return "NO_MATCH"

def _extract_location_info(soup):
    """
    Extrait les informations de localisation depuis la page Numbeo.
    Retourne un tuple (ville, pays, état) ou (None, None, None) si pas trouvé.
    """
    # 1. Essayer d'extraire depuis le breadcrumb
    breadcrumb = soup.find('nav', class_='breadcrumb')
    if breadcrumb:
        breadcrumb_links = breadcrumb.find_all('a', class_='breadcrumb_link')
        if len(breadcrumb_links) >= 3:
            # Ordre Numbeo : catégorie > pays > ville
            country = breadcrumb_links[1].get_text(strip=True)  # 2ème élément = pays
            city_full = breadcrumb_links[2].get_text(strip=True)     # 3ème élément = ville
            
            # Nettoyer le nom de la ville (retirer la partie après la virgule)
            city = city_full.split(',')[0].strip()
            
            # Extraire l'état si présent dans le nom de la ville
            state = None
            if ',' in city_full:
                state_part = city_full.split(',')[1].strip()
                # Si ce n'est pas le pays, c'est l'état
                if state_part.lower() != country.lower():
                    state = state_part
            
            return city, country, state
    
    # 2. Essayer d'extraire depuis le titre h1
    h1 = soup.find('h1')
    if h1:
        title_text = h1.get_text(strip=True)
        # Format: "Quality of Life in Paris, France" ou "Quality of Life in Washington, DC, United States"
        if ' in ' in title_text:
            location_part = title_text.split(' in ')[1]
            parts = [p.strip() for p in location_part.split(',')]
            
            if len(parts) >= 2:
                city = parts[0]  # Première partie = ville
                country = parts[-1]  # Dernier élément = pays
                state = parts[1] if len(parts) > 2 else None  # État au milieu si présent
                return city, country, state
    
    # 3. Essayer d'extraire depuis les champs cachés du formulaire
    city_input = soup.find('input', {'id': 'city'})
    country_input = soup.find('input', {'id': 'country'})
    
    if city_input and country_input:
        city = city_input.get('value', '').strip()
        country = country_input.get('value', '').strip()
        return city, country, None
    
    return None, None, None

# --- MODULE: Extraction des liens de catégories ---
def extract_category_links(city_url):
    logger.debug(f"Extraction des liens de catégories depuis {city_url}")
    response = requests.get(city_url, headers=HEADERS)
    logger.debug(f"Réponse HTTP {response.status_code} pour {city_url}")
    soup = BeautifulSoup(response.content, "html.parser")
    categories = {
        "quality_of_life": "quality-of-life",
        "crime": "crime",
        "cost_of_living": "cost-of-living",
        "health_care": "health-care",
        "climate": "climate",
        "property_investment": "property-investment",
        "traffic": "traffic",
        "pollution": "pollution"
    }
    links = {}
    # Extract city name part from the URL
    city_name = city_url.split("/in/")[-1]
    for cat, keyword in categories.items():
        found = False
        for a in soup.find_all("a", href=True):
            href = a['href']
            if f"/{keyword}/in/" in href:
                links[cat] = BASE_URL + href if href.startswith("/") else href
                found = True
                break
        if not found:
            # Always build the URL if not found
            links[cat] = f"{BASE_URL}/{keyword}/in/{city_name}"
    return links

# --- MODULE: Scraping des tables avec la classe spécifique ---
def scrape_selected_tables(page_url):
    logger.debug(f"Scraping tables sur {page_url}")
    response = requests.get(page_url, headers=HEADERS)
    logger.debug(f"Réponse HTTP {response.status_code} pour {page_url}")
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table", class_="table_builder_with_value_explanation")
    logger.debug(f"{len(tables)} tables trouvées sur {page_url}")
    dataframes = []
    sheet_names = []
    for idx, table in enumerate(tables):
        # Try to get the caption or a title above the table
        caption = None
        if table.caption and table.caption.text.strip():
            caption = table.caption.text.strip()
        else:
            # Try to find a preceding <h2> or <h3> as a title
            prev = table.find_previous(["h2", "h3"])
            if prev and prev.text.strip():
                caption = prev.text.strip()
        # Fallback to Table1, Table2, ...
        if not caption:
            caption = f"Table{idx+1}"
        # Clean caption for Excel sheet name
        safe_caption = re.sub(r'[\\/*?:\[\]]', '', caption)[:31]
        logger.debug(f"Extraction de la table {idx+1} (caption: {caption})")
        try:
            df = pd.read_html(str(table))[0]
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une table en DataFrame: {e}")
    return dataframes, sheet_names

# --- MODULE: Sauvegarde des données dans un fichier CSV ---
def save_city_data_csv(country_name, city_name, category, tables_and_names):
    tables, sheet_names = tables_and_names
    if not tables:
        logger.debug(f"Aucune table à sauvegarder pour {country_name}, {city_name}, {category}")
        return
    safe_country = country_name.replace(" ", "_")
    safe_city = city_name.replace(" ", "_")
    
    # Pour chaque table, créer un fichier CSV séparé
    for i, (table, sheet_name) in enumerate(zip(tables, sheet_names)):
        # Nettoyer le nom de fichier pour CSV
        safe_sheet_name = re.sub(r'[\\/*?:\[\]]', '', sheet_name)
        filename = f"{TIMESTAMPED_OUTPUT_DIR}/{safe_country}_{safe_city}_{category}_{safe_sheet_name}.csv"
        logger.debug(f"Sauvegarde du DataFrame dans {filename}")
        table.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"✅ Données sauvegardées dans {filename}")

# --- MODULE: Scraping du tableau principal de la page Quality of Life ---
def scrape_quality_of_life_summary(page_url):
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    main_table = None
    for table in soup.find_all("table"):
        # Exclude tables inside <aside>
        if table.find_parent("aside") is None:
            rows = table.find_all("tr")
            if len(rows) >= 5 and any(len(r.find_all("td")) == 3 for r in rows):
                main_table = table
                break
    if not main_table:
        print(f'[DEBUG] Aucun tableau trouvé pour {page_url}')
        debug_filename = f'output/debug_Lyon.html'
        with open(debug_filename, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        logger.warning(f"Dump HTML sauvegardé dans {debug_filename} pour analyse (aucun tableau trouvé pour {page_url})")
        return None, None
    try:
        df = pd.read_html(str(main_table))[0]
        # Try to get a caption or fallback name
        caption = None
        if main_table.caption and main_table.caption.text.strip():
            caption = main_table.caption.text.strip()
        else:
            prev = main_table.find_previous(["h2", "h3"])
            if prev and prev.text.strip():
                caption = prev.text.strip()
        if not caption:
            caption = "QualityOfLifeSummary"
        safe_caption = re.sub(r'[\\/*?:\[\]]', '', caption)[:31]
        return df, safe_caption
    except Exception as e:
        print(f"Erreur lors de la conversion du tableau principal: {e}")
        return None, None

# --- MODULE: Scraping spécifique pour la catégorie traffic ---
def scrape_traffic_tables(page_url):
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    dataframes = []
    sheet_names = []
    # 1. Table des indices globaux
    indices_table = soup.find("table", class_="table_indices")
    if indices_table:
        try:
            df = pd.read_html(str(indices_table))[0]
            dataframes.append(df)
            sheet_names.append("Indices")
        except Exception as e:
            print(f"Erreur lors de la conversion de la table indices: {e}")
    # 2. Tables suivant les titres <h3> spécifiques
    h3_titles = [
        "Main Means of Transportation to Work or School",
        "Overall Average One-Way Commute Time and Distance to Work or School",
        "Average when primarily using Walking",
        "Average when primarily using Car",
        "Average when primarily using Bicycle",
        "Average when primarily using Bus/Trolleybus",
        "Average when primarily using Tram/Streetcar",
        "Average when primarily using Train/Metro"
    ]
    for h3 in soup.find_all("h3"):
        if h3.text.strip() in h3_titles:
            next_table = h3.find_next_sibling("table")
            if next_table:
                try:
                    df = pd.read_html(str(next_table))[0]
                    # Nettoyer le nom de l'onglet
                    safe_caption = re.sub(r'[\\/*?:\[\]]', '', h3.text.strip())[:31]
                    dataframes.append(df)
                    sheet_names.append(safe_caption)
                except Exception as e:
                    print(f"Erreur lors de la conversion d'une table traffic: {e}")
    return dataframes, sheet_names

# --- MODULE: Scraping spécifique pour la catégorie cost_of_living ---
def scrape_cost_of_living_tables(page_url):
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    dataframes = []
    sheet_names = []
    tables = soup.find_all("table", class_="data_wide_table")
    for idx, table in enumerate(tables):
        try:
            df = pd.read_html(str(table))[0]
            # Utilise le caption ou un nom générique
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
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une table cost_of_living: {e}")
    return dataframes, sheet_names

# --- MODULE: Scraping spécifique pour la catégorie property_investment ---
def scrape_property_investment_tables(page_url):
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    dataframes = []
    sheet_names = []
    # Tables avec la classe 'table_indices'
    indices_tables = soup.find_all("table", class_="table_indices")
    for idx, table in enumerate(indices_tables):
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
                caption = f"Indices{idx+1}"
            safe_caption = re.sub(r'[\\/*?:\[\]]', '', caption)[:31]
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une table_indices property_investment: {e}")
    # Tables avec la classe 'data_wide_table'
    wide_tables = soup.find_all("table", class_="data_wide_table")
    for idx, table in enumerate(wide_tables):
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
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une data_wide_table property_investment: {e}")
    return dataframes, sheet_names

# --- MODULE: Scraping spécifique pour la catégorie climate ---
def scrape_climate_tables(page_url):
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    dataframes = []
    sheet_names = []
    # 1. Toutes les tables
    tables = soup.find_all("table")
    for idx, table in enumerate(tables):
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
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une table climate: {e}")
    # 2. Toutes les div.tempChartDiv (sauvegardées comme texte ou HTML)
    chart_divs = soup.find_all("div", class_="tempChartDiv")
    for idx, div in enumerate(chart_divs):
        # On sauvegarde le HTML brut dans un DataFrame à une colonne
        html_content = div.decode_contents()
        df = pd.DataFrame({"tempChartDiv_html": [html_content]})
        sheet_name = f"TempChartDiv{idx+1}"
        dataframes.append(df)
        sheet_names.append(sheet_name)
    return dataframes, sheet_names

# --- MAIN ---
def main():
    # --- SÉCURITÉ : Vérification du fichier cities.csv ---
    cities_path = os.path.join('datas', 'cities.csv')
    if not os.path.isfile(cities_path):
        logger.error("Le fichier 'datas/cities.csv' est introuvable. Merci de fournir ce fichier avant de lancer le script.")
        print("[ERREUR] Le fichier 'datas/cities.csv' est introuvable. Merci de fournir ce fichier avant de lancer le script.")
        return
    try:
        cities_df = pd.read_csv(cities_path)
        if cities_df.empty or len(cities_df.dropna(how='all')) == 0:
            logger.error("Le fichier 'datas/cities.csv' est vide. Merci de le remplir avant de lancer le script.")
            print("[ERREUR] Le fichier 'datas/cities.csv' est vide. Merci de le remplir avant de lancer le script.")
            return
    except Exception as e:
        logger.error(f"Impossible de lire 'datas/cities.csv' : {e}")
        print(f"[ERREUR] Impossible de lire 'datas/cities.csv' : {e}")
        return
    # --- Suite du process normal ---
    logger.info(f"Nombre de villes chargées : {len(cities_df)}")
    cities_df = load_cities()
    cities_df = cities_df.dropna(subset=['city', 'country'])
    cities_df = cities_df[cities_df['city'].str.strip() != '']
    print(f'Nombre de villes chargées : {len(cities_df)}')
    for _, row in cities_df.iterrows():
        print(f'Ville: {row.get("city")}, Pays: {row.get("country")}')
        city = row['city']
        country = row['country']
        state = row.get('state', None)
        city_url = find_valid_city_url(city, country, state)
        if not city_url:
            print(f"❌ Aucune page Numbeo trouvée pour {city}, {country}, {state}")
            continue
        print(f"Scraping {city} → {city_url}")
        # Scrape and save the main summary table from the Quality of Life page
        summary_df, summary_caption = scrape_quality_of_life_summary(city_url)
        if summary_df is not None:
            safe_country = country.replace(" ", "_")
            safe_city = city.replace(" ", "_")
            filename = f"{TIMESTAMPED_OUTPUT_DIR}/{safe_country}_{safe_city}_quality_of_life_summary.csv"
            summary_df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Tableau principal sauvegardé dans {filename}")
        # Continue with category links as before
        category_links = extract_category_links(city_url)
        for category, url in category_links.items():
            print(f"  - Scraping {category} → {url}")
            if category == "traffic":
                tables_and_names = scrape_traffic_tables(url)
            elif category == "cost_of_living":
                tables_and_names = scrape_cost_of_living_tables(url)
            elif category == "property_investment":
                tables_and_names = scrape_property_investment_tables(url)
            elif category == "climate":
                tables_and_names = scrape_climate_tables(url)
            else:
                tables_and_names = scrape_selected_tables(url)
            save_city_data_csv(country, city, category, tables_and_names)
            sleep_time = random.uniform(20, 40)
            print(f"    ⏳ Pause de {sleep_time:.2f} secondes...")
            time.sleep(sleep_time)
        # Extra pause between cities
        city_sleep = random.uniform(30, 60)
        print(f"⏳ Pause de {city_sleep:.2f} secondes avant la prochaine ville...")
        time.sleep(city_sleep)
    # --- Fin du process ---
    print("Mission terminée : le process s'arrête automatiquement avec succès.")
    logger.info("Mission terminée : le process s'arrête automatiquement avec succès.")
    sys.exit(0)

if __name__ == "__main__":
    main()


