import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import json

# Load environment variables
load_dotenv()
SUPABASE_URL = "https://hvhpfjkiesiaygxycynt.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TABLE_NAME = "cities_scraped"
CSV_PATH = "ALL_DATA_CONCATENATED.csv"  # Change path if needed

# --- Helper: Connect to Supabase Postgres directly for DDL (table creation) ---
def get_postgres_conn():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise Exception("Set SUPABASE_DB_URL in your .env (see Supabase > Project Settings > Database > Connection string)")
    return psycopg2.connect(db_url)

# --- Step 1: Read the merged CSV ---
def read_csv_schema(csv_path):
    df = pd.read_csv(csv_path)
    # Standard columns
    std_cols = ["city", "country", "region", "category", "table_caption", "imported_at", "source_file"]
    # All columns, in order: std_cols first, then the rest
    all_cols = std_cols + [c for c in df.columns if c not in std_cols]
    return df, all_cols

# --- Step 2: Create table if not exists ---
def create_table_if_needed(conn, table_name, columns):
    cur = conn.cursor()
    # Build SQL for columns
    column_types = {
        "city": "text NOT NULL",
        "country": "text",
        "region": "text",
        "category": "text NOT NULL",
        "table_caption": "text",
        "imported_at": "timestamp NOT NULL",
        "source_file": "text",
        "data_json": "jsonb"
    }
    col_defs = []
    for col in columns:
        col_type = column_types.get(col, "text")
        col_defs.append(f'"{col}" {col_type}')
    # Add the data_json column
    col_defs.append('"data_json" jsonb')
    sql = f'''CREATE TABLE IF NOT EXISTS "{table_name}" (
        id bigserial primary key,
        {', '.join(col_defs)}
    );'''
    cur.execute(sql)
    conn.commit()
    cur.close()
    print(f"✅ Table '{table_name}' checked/created.")

# --- Step 3: Insert data ---
def insert_data(conn, table_name, df, columns):
    cur = conn.cursor()
    # Prepare insert statement
    col_names = ', '.join(f'"{c}"' for c in columns + ["data_json"])
    placeholders = ', '.join(['%s'] * (len(columns) + 1))
    insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders});'
    # Insert rows
    for row in df[columns].itertuples(index=False, name=None):
        row_dict = dict(zip(columns, row))
        data_json = json.dumps(row_dict, ensure_ascii=False)
        try:
            cur.execute(insert_sql, (*row, data_json))
        except Exception as e:
            print(f"Error inserting row: {e}")
    conn.commit()
    cur.close()
    print(f"✅ Data inserted into '{table_name}'.")

if __name__ == "__main__":
    # Step 1: Read CSV
    df, all_cols = read_csv_schema(CSV_PATH)
    print(f"Read {len(df)} rows from {CSV_PATH}")
    # Step 2: Connect to Supabase Postgres
    conn = get_postgres_conn()
    # Step 3: Create table if needed
    create_table_if_needed(conn, TABLE_NAME, all_cols)
    # Step 4: Insert data
    insert_data(conn, TABLE_NAME, df, all_cols)
    conn.close()
    print("✅ All done!") 