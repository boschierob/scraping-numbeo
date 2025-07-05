from scrape_numbeo import load_cities, build_numbeo_url, extract_category_links, scrape_main_table, save_city_data
import os
import pandas as pd

def test_load_cities():
    df = load_cities()
    assert not df.empty, "La liste des villes ne doit pas être vide"
    print("✅ Chargement des villes OK")
    print(df.head())

def test_build_numbeo_url():
    url = build_numbeo_url("Paris")
    assert "Paris" in url, "URL mal construite"
    print("✅ Construction URL OK :", url)

def test_extract_category_links():
    url = build_numbeo_url("Paris")
    links = extract_category_links(url)
    assert isinstance(links, dict), "La fonction doit retourner un dictionnaire"
    # Should contain at least the main categories
    expected = [
        "quality_of_life", "crime", "cost_of_living", "health_care",
        "climate", "property_investment", "traffic", "pollution"
    ]
    found = [cat for cat in expected if cat in links]
    assert found, "Aucun lien de catégorie trouvé"
    print(f"✅ Extraction des liens OK : {found}")
    print(links)

def test_scrape_main_table():
    # Test with the crime page for Paris
    url = "https://www.numbeo.com/crime/in/Paris"
    data = scrape_main_table(url)
    assert isinstance(data, list) or data is None, "La fonction doit retourner une liste ou None"
    if data:
        first = data[0]
        assert 'item' in first and 'value' in first and 'label' in first, "Chaque ligne doit avoir les clés 'item', 'value', 'label'"
        print(f"✅ Scraping OK : {len(data)} lignes extraites pour Paris (crime)")
        print(data[:2])
    else:
        print("⚠️ Pas de données extraites pour Paris (crime)")

def test_save_city_data():
    # Mock data for crime
    data = [
        {"item": "Crime Index", "value": "52.85", "label": "Moderate"},
        {"item": "Safety Index", "value": "47.15", "label": "Moderate"}
    ]
    country = "France"
    city = "Paris"
    category = "crime"
    filename = f"output/{country.replace(' ', '_')}_{city.replace(' ', '_')}_{category}.csv"
    # Supprime le fichier s'il existe
    if os.path.exists(filename):
        os.remove(filename)
    save_city_data(country, city, category, data)
    assert os.path.exists(filename), "Le fichier CSV n'a pas été créé"
    df = pd.read_csv(filename)
    assert not df.empty, "Le fichier CSV est vide"
    # Check columns
    assert set(['item', 'value', 'label']).issubset(df.columns), "Le CSV doit contenir les colonnes 'item', 'value', 'label'"
    print("✅ Sauvegarde CSV OK")
    # Nettoyage
    os.remove(filename)

if __name__ == "__main__":
    test_load_cities()
    test_build_numbeo_url()
    test_extract_category_links()
    test_scrape_main_table()
    test_save_city_data()
