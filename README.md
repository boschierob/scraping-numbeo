# Numbeo Scraping

Scrape, standardize, and upload Numbeo data (cost of living, climate, crime, health, etc.) for any city, with export to CSV/JSON and automated import into Supabase.

---

## Features
- Multi-category scraping (cost of living, climate, crime, health, etc.)
- Interactive CLI: scrape from CSV, URLs, or city slugs
- Web interface (Flask) for scraping by URL
- Standardized data structure and column mapping
- Export results as CSV or JSON
- Automatic merging of session files
- Automated Supabase table creation and data import
- Detailed logging

---

## Installation
1. Clone the repo
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure your `.env` file:
   ```ini
   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
   SUPABASE_DB_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE
   ```

---

## Usage

### 1. Scraping (interactive CLI)
```bash
python main.py
```
- Choose: scrape from CSV, URLs, or city slugs

### 2. Merge all CSVs from a session
```bash
python concatenate_city_csvs.py output/YourSessionFolder
```

### 3. Create (or reset) the Supabase table
```bash
python create_supabase_table.py
```

### 4. Import merged data into Supabase
```bash
python upload_to_supabase.py
```

### 5. Web interface (Flask)
```bash
python flask_app.py
```

---

## Data Structure
- All exports (CSV/JSON) use a standardized schema:
  - city, country, region, category, table_caption, imported_at, item, value, value2, value3, note, data_type, ...
- All data can also be stored as JSON for flexibility.

---

## Customization
- Add new categories: update column mapping in the code
- Change export format: modify save functions (CSV/JSON)
- Change Supabase schema: edit `create_supabase_table.py`

---

## Dependencies
- Python 3.8+
- pandas, requests, beautifulsoup4, supabase-py, psycopg2-binary, flask, python-dotenv

---

## License
MIT

---

## Maintainer
- For questions or contributions, open an issue or contact the maintainer. 