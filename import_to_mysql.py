import os
import glob
import pandas as pd
import mysql.connector
from datetime import datetime
import logging
from dotenv import load_dotenv
from mysql.connector import IntegrityError

# Charger les variables d'environnement depuis .env
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'import_mysql.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logging.info("Démarrage du script import_to_mysql.py")

# Paramètres de connexion
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_DB = os.environ.get("MYSQL_DB", "bobr5923_cities")
MYSQL_USER = os.environ.get("MYSQL_USER", "bobr5923_bruno")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")  # Doit être défini dans .env

TABLE_NAME = "scraped_data"

BASE_COLUMNS = [
    ("city", "VARCHAR(128)"),
    ("country", "VARCHAR(128)"),
    ("region", "VARCHAR(128)"),
    ("category", "VARCHAR(128)"),
    ("table_caption", "VARCHAR(255)"),
    ("imported_at", "DATETIME")
]

def create_table_if_not_exists(conn, extra_columns):
    cur = conn.cursor()
    columns_sql = ", ".join([f"`{col}` {ctype}" for col, ctype in BASE_COLUMNS])
    for col in extra_columns:
        if col not in [c[0] for c in BASE_COLUMNS]:
            columns_sql += f", `{col}` TEXT"
    unique_sql = "UNIQUE KEY unique_entry (city, country, region, category, table_caption, imported_at)"
    sql = f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` ({columns_sql}, {unique_sql}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    try:
        cur.execute(sql)
        logging.info(f"Table '{TABLE_NAME}' vérifiée/créée avec schéma dynamique.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table : {e}")
    finally:
        cur.close()
        conn.commit()

def import_csv_to_mysql(csv_path, city, country, region, category, imported_at, conn):
    logging.info(f"Début import du fichier : {csv_path}")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logging.error(f"Erreur lecture CSV {csv_path} : {e}")
        return 0, 0, 0, 0
    df["city"] = city
    df["country"] = country
    df["region"] = region
    df["category"] = category
    df["imported_at"] = imported_at
    if "table_caption" not in df.columns:
        df["table_caption"] = None

    create_table_if_not_exists(conn, df.columns)

    # Suppression des anciennes données pour cette ville/catégorie
    cur = conn.cursor()
    try:
        delete_sql = f"DELETE FROM `{TABLE_NAME}` WHERE city=%s AND country=%s AND region=%s AND category=%s"
        cur.execute(delete_sql, (city, country, region, category))
        conn.commit()
        logging.info(f"Anciennes données supprimées pour {city}, {country}, {region}, {category}")
    except Exception as e:
        logging.error(f"Erreur suppression anciennes données : {e}")

    # Insertion des nouvelles données
    cols = list(df.columns)
    insert_sql = f"INSERT INTO `{TABLE_NAME}` ({', '.join('`'+c+'`' for c in cols)}) VALUES ({', '.join(['%s']*len(cols))})"
    inserted = 0
    insert_errors = 0
    duplicate_errors = 0
    for row in df.itertuples(index=False, name=None):
        try:
            cur.execute(insert_sql, row)
            inserted += 1
        except IntegrityError as ie:
            logging.error(f"Clé dupliquée ou contrainte violée lors de l'insertion : {ie}")
            duplicate_errors += 1
            insert_errors += 1
        except Exception as e:
            logging.error(f"Erreur insertion ligne : {e}")
            insert_errors += 1
    conn.commit()
    cur.close()
    logging.info(f"✅ {inserted} lignes insérées depuis {csv_path} | {duplicate_errors} doublons détectés.")
    return inserted, insert_errors, duplicate_errors, len(df)

def parse_context_from_path(csv_path):
    parts = os.path.normpath(csv_path).split(os.sep)
    for i, part in enumerate(parts):
        if part == "output" and i+1 < len(parts):
            folder = parts[i+1]
            folder_main = folder.rsplit("-", 1)[0]
            tokens = folder_main.split("-")
            if len(tokens) == 3:
                city, region, country = tokens
            elif len(tokens) == 2:
                city, country = tokens
                region = ""
            else:
                city = tokens[0]
                region = ""
                country = ""
            return city, country, region
    return "UnknownCity", "UnknownCountry", ""

def import_all_csvs(output_dir):
    logging.info(f"Début import MySQL pour tous les CSV du dossier : {output_dir}")
    try:
        logging.info("Tentative de connexion à la base de données MySQL...")
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset="utf8mb4"
        )
        logging.info("Connexion à la base de données MySQL réussie.")
    except Exception as e:
        logging.critical(f"Échec de connexion à la base MySQL : {e}")
        import sys
        sys.exit(1)
    imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    csv_files = list(glob.glob(os.path.join(output_dir, "**", "*.csv"), recursive=True))
    total_files = len(csv_files)
    if total_files == 0:
        logging.critical(f"Aucun fichier CSV trouvé dans {output_dir}. Import annulé.")
        conn.close()
        import sys
        sys.exit(1)
    logging.info(f"{total_files} fichiers CSV trouvés à importer.")
    imported = 0
    failed = 0
    total_lines = 0
    total_insert_errors = 0
    total_duplicate_errors = 0
    total_rows = 0
    # Rapport détaillé par (ville, catégorie)
    detailed_report = {}
    for idx, csv_path in enumerate(csv_files, 1):
        logging.info(f"[Fichier {idx}/{total_files}] Début import : {csv_path}")
        city, country, region = parse_context_from_path(csv_path)
        category = os.path.basename(os.path.dirname(csv_path))
        key = (city, country, region, category)
        try:
            inserted, insert_errors, duplicate_errors, num_rows = import_csv_to_mysql(csv_path, city, country, region, category, imported_at, conn)
            logging.info(f"[Fichier {idx}/{total_files}] Fin import : {csv_path}")
            imported += 1
            total_lines += inserted
            total_insert_errors += insert_errors
            total_duplicate_errors += duplicate_errors
            total_rows += num_rows
            if key not in detailed_report:
                detailed_report[key] = {'inserted': 0, 'insert_errors': 0, 'duplicate_errors': 0, 'rows': 0}
            detailed_report[key]['inserted'] += inserted
            detailed_report[key]['insert_errors'] += insert_errors
            detailed_report[key]['duplicate_errors'] += duplicate_errors
            detailed_report[key]['rows'] += num_rows
        except Exception as e:
            logging.error(f"Erreur critique lors de l'import du fichier {csv_path} : {e}")
            failed += 1
    conn.close()
    logging.info(f"✅ Import MySQL terminé pour {output_dir} : {imported} fichiers importés, {failed} échecs.")
    logging.info(f"Résumé : {total_lines} lignes insérées, {total_insert_errors} erreurs d'insertion dont {total_duplicate_errors} doublons détectés, {total_rows} lignes lues au total.")
    # Rapport détaillé par ville/catégorie
    logging.info("--- Rapport détaillé par ville/catégorie ---")
    for (city, country, region, category), stats in detailed_report.items():
        logging.info(f"Ville: {city}, Pays: {country}, Région: {region}, Catégorie: {category} | Lignes insérées: {stats['inserted']}, Erreurs: {stats['insert_errors']}, Doublons: {stats['duplicate_errors']}, Lignes lues: {stats['rows']}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        logging.error("Usage: python import_to_mysql.py <output_folder>")
    else:
        import_all_csvs(sys.argv[1]) 