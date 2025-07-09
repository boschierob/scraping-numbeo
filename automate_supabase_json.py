import os
import glob
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import sys

# --- CONFIGURATION ---
TABLE_NAME = "cities_scraped"
# .env must contain SUPABASE_DB_URL

load_dotenv()

def get_postgres_conn():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise Exception("Set SUPABASE_DB_URL in your .env (see Supabase > Project Settings > Database > Connection string)")
    return psycopg2.connect(db_url)

def create_table_if_needed(conn):
    cur = conn.cursor()
    sql = f'''
    CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (
        id bigserial PRIMARY KEY,
        city text NOT NULL,
        country text,
        region text,
        datestamp timestamp NOT NULL,
        data_json jsonb,
        UNIQUE(city, region, datestamp)
    );
    '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    print(f"✅ Table '{TABLE_NAME}' checked/created (no drop).")

def collect_city_data(city_folder, city, country, datestamp, region=None):
    city_data = {
        "city": city,
        "country": country,
        "datestamp": datestamp
    }
    if region:
        city_data["region"] = region
    # Parcours des sous-dossiers (catégories)
    for category in os.listdir(city_folder):
        category_path = os.path.join(city_folder, category)
        if not os.path.isdir(category_path):
            continue
        csv_files = glob.glob(os.path.join(category_path, "*.csv"))
        if not csv_files:
            print(f"[DEBUG] Aucun CSV trouvé pour la catégorie {category}")
            continue
        print(f"[DEBUG] {len(csv_files)} CSV trouvés pour la catégorie {category}")
        dfs = []
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                dfs.append(df)
            except Exception as e:
                print(f"[DEBUG] Erreur lecture {file} : {e}")
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            city_data[category] = json.loads(merged.to_json(orient='records', force_ascii=False))
        else:
            print(f"[DEBUG] Aucun DataFrame valide pour la catégorie {category}")
    return city_data

def insert_city_json(conn, city_data):
    cur = conn.cursor()
    insert_sql = f'''
    INSERT INTO "{TABLE_NAME}" (city, country, region, datestamp, data_json)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (city, region, datestamp)
    DO UPDATE SET
        country = EXCLUDED.country,
        data_json = EXCLUDED.data_json;
    '''
    try:
        cur.execute(
            insert_sql,
            (
                city_data.get("city"),
                city_data.get("country"),
                city_data.get("region"),
                city_data.get("datestamp"),
                json.dumps(city_data, ensure_ascii=False)
            )
        )
        conn.commit()
        print(f"✅ Data for {city_data.get('city')} inserted/updated in '{TABLE_NAME}'.")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cur.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python automate_supabase_json.py <session_folder> <city> [region] <country>")
        sys.exit(1)
    session_folder = sys.argv[1]
    city = sys.argv[2]
    if len(sys.argv) == 5:
        region = sys.argv[3]
        country = sys.argv[4]
    else:
        region = None
        country = sys.argv[3]
    datestamp = datetime.now().isoformat()

    print(f"🔎 Collecting data from {session_folder} for {city}, {country}, {region or ''} ...")
    city_json = collect_city_data(session_folder, city, country, datestamp, region)

    # Optionally save the JSON locally for debug
    with open(os.path.join(session_folder, "city_data.json"), "w", encoding="utf-8") as f:
        json.dump(city_json, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON saved to {os.path.join(session_folder, 'city_data.json')}")

    # Insert into Supabase
    conn = get_postgres_conn()
    create_table_if_needed(conn)
    insert_city_json(conn, city_json)
    conn.close()
    print("✅ All done!") 