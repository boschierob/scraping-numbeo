#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Configuration pour le déploiement sur o2switch
"""

import os
from pathlib import Path

# Configuration du projet
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "datas"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"

# Créer les dossiers nécessaires
for directory in [DATA_DIR, OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

# Configuration MySQL pour o2switch
MYSQL_CONFIG = {
    'host': 'localhost',  # Sur o2switch, MySQL est en localhost
    'port': 3306,
    'user': 'bobr5923_bruno',
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': 'bobr5923_cities',
    'charset': 'utf8mb4'
}

# Configuration Streamlit pour o2switch
STREAMLIT_CONFIG = {
    'server.port': 8501,
    'server.address': '0.0.0.0',
    'server.headless': True,
    'server.enableCORS': False,
    'server.enableXsrfProtection': False,
    'browser.gatherUsageStats': False
}

# Configuration des chemins de fichiers
FILES_CONFIG = {
    'users_file': DATA_DIR / "users.json",
    'cities_csv': DATA_DIR / "cities.csv",
    'import_log': PROJECT_ROOT / "import_mysql.log",
    'scraping_log': LOGS_DIR / "scraping.log"
}

print("Configuration o2switch chargée avec succès!")
print(f"Répertoire projet: {PROJECT_ROOT}")
print(f"Répertoire données: {DATA_DIR}")
print(f"Répertoire sortie: {OUTPUT_DIR}") 