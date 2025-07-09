import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
TABLE_NAME = "cities_scraped"

# SQL for dropping and creating the table
CREATE_TABLE_SQL = f'''
DROP TABLE IF EXISTS "{TABLE_NAME}";
CREATE TABLE "{TABLE_NAME}" (
    id bigserial PRIMARY KEY,
    city text NOT NULL,
    country text,
    region text,
    category text NOT NULL,
    table_caption text,
    imported_at timestamp NOT NULL,
    source_file text,
    data_json jsonb
);
'''

def get_postgres_conn():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise Exception("Set SUPABASE_DB_URL in your .env (see Supabase > Project Settings > Database > Connection string)")
    return psycopg2.connect(db_url)

if __name__ == "__main__":
    conn = get_postgres_conn()
    cur = conn.cursor()
    try:
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        print(f"✅ Table '{TABLE_NAME}' dropped and recreated.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close() 