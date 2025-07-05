import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import os
import pandas as pd
import random
from urllib.parse import quote
import re

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
    return url

# --- MODULE: Extraction des liens de catégories ---
def extract_category_links(city_url):
    response = requests.get(city_url, headers=HEADERS)
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
    response = requests.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table", class_="table_builder_with_value_explanation")
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
        try:
            df = pd.read_html(str(table))[0]
            dataframes.append(df)
            sheet_names.append(safe_caption)
        except Exception as e:
            print(f"Erreur lors de la conversion d'une table en DataFrame: {e}")
    return dataframes, sheet_names

# --- MODULE: Sauvegarde des données dans un fichier Excel ---
def save_city_data_excel(country_name, city_name, category, tables_and_names):
    tables, sheet_names = tables_and_names
    if not tables:
        return
    safe_country = country_name.replace(" ", "_")
    safe_city = city_name.replace(" ", "_")
    filename = f"{TIMESTAMPED_OUTPUT_DIR}/{safe_country}_{safe_city}_{category}.xlsx"
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for table, sheet_name in zip(tables, sheet_names):
            table.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"✅ Données sauvegardées dans {filename}")

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
        with open(f'output/debug_Lyon.html', 'w', encoding='utf-8') as f:
            f.write(str(soup))
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
    cities_df = load_cities()
    cities_df = cities_df.dropna(subset=['city', 'country'])
    cities_df = cities_df[cities_df['city'].str.strip() != '']
    print(f'Nombre de villes chargées : {len(cities_df)}')
    for _, row in cities_df.iterrows():
        print(f'Ville: {row.get("city")}, Pays: {row.get("country")}')
        city = row['city']
        country = row['country']
        city_url = build_numbeo_url(city)
        print(f"Scraping {city} → {city_url}")
        # Scrape and save the main summary table from the Quality of Life page
        summary_df, summary_caption = scrape_quality_of_life_summary(city_url)
        if summary_df is not None:
            safe_country = country.replace(" ", "_")
            safe_city = city.replace(" ", "_")
            filename = f"{TIMESTAMPED_OUTPUT_DIR}/{safe_country}_{safe_city}_quality_of_life_summary.xlsx"
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                summary_df.to_excel(writer, sheet_name=summary_caption, index=False)
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
            save_city_data_excel(country, city, category, tables_and_names)
            sleep_time = random.uniform(20, 40)
            print(f"    ⏳ Pause de {sleep_time:.2f} secondes...")
            time.sleep(sleep_time)
        # Extra pause between cities
        city_sleep = random.uniform(30, 60)
        print(f"⏳ Pause de {city_sleep:.2f} secondes avant la prochaine ville...")
        time.sleep(city_sleep)

if __name__ == "__main__":
    main()


