#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, render_template, render_template_string, request, redirect, flash, session, url_for
import os
import sys
import json
import hashlib
from pathlib import Path
from main import scrape_from_url, automate_supabase_for_all_outputs
from dotenv import load_dotenv
load_dotenv()
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"[DEBUG] CWD set to: {os.getcwd()}")
print('[DEBUG] SUPABASE_DB_URL (Flask) =', os.getenv('SUPABASE_DB_URL'))

# Ajouter le répertoire du projet au PYTHONPATH
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Changez ceci en production

# Configuration
USERS_FILE = os.path.join("datas", "users.json")

def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def check_login(username, password):
    if not os.path.exists(USERS_FILE):
        return False
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        users = json.load(f)
    return username in users and users[username] == hash_password(password)

# Template HTML simple
# SUPPRIMER HTML_TEMPLATE et remplacer la route index

@app.route('/')
def index():
    inserted_rows = session.pop('inserted_rows', None)
    insert_errors = session.pop('insert_errors', None)
    return render_template('index.html', inserted_rows=inserted_rows, insert_errors=insert_errors)

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')
    
    if check_login(username, password):
        session['logged_in'] = True
        session['username'] = username
        flash('Connexion réussie!', 'success')
    else:
        flash('Identifiant ou mot de passe incorrect.', 'error')
    
    return redirect('/')

@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    flash('Déconnexion réussie.', 'success')
    return redirect('/')

@app.route('/scrape_urls', methods=['POST'])
def scrape_urls():
    urls = request.form.get('urls', '').strip()
    if not urls:
        flash('Veuillez entrer au moins une URL.', 'warning')
        return redirect('/')

    created_folders = []
    inserted_rows = []
    errors = []
    for url in [u.strip() for u in urls.splitlines() if u.strip()]:
        output_dir = Path('output')
        print(f"[DEBUG] Absolute output dir: {output_dir.resolve()}")
        before = set(f.name for f in output_dir.iterdir() if f.is_dir())
        print(f"[DEBUG] Output folders before scraping: {before}")
        try:
            scrape_from_url(url)
            flash(f"✅ Succès pour {url}", "success")
        except Exception as e:
            flash(f"❌ Erreur pour {url} : {e}", "error")
        print(f"[DEBUG] Absolute output dir (after): {output_dir.resolve()}")
        print(f"[DEBUG] Files/folders in output dir: {list(output_dir.iterdir())}")
        for f in output_dir.iterdir():
            print(f"[DEBUG] {f} - is_dir: {f.is_dir()} - is_file: {f.is_file()}")
        after = set(f.name for f in output_dir.iterdir() if f.is_dir())
        print(f"[DEBUG] Output folders after scraping: {after}")
        new_folders = after - before
        print(f"[DEBUG] New folders detected: {new_folders}")
        created_folders.extend([output_dir/f for f in new_folders])
    print(f"[DEBUG] created_folders list: {created_folders}")
    if created_folders:
        # On va collecter les infos d'insertion pour l'affichage
        from automate_supabase_json import collect_city_data
        for city_folder in created_folders:
            meta_path = city_folder / "meta.json"
            if meta_path.exists():
                import json
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                city = meta.get("city")
                country = meta.get("country")
            else:
                # fallback parsing
                parts = city_folder.name.split('-')
                city = parts[0]
                country = parts[-2] if len(parts) > 2 else parts[-1]
            try:
                # On tente l'insertion (déjà faite, mais on veut le feedback)
                print(f"[DEBUG] Appel automate_supabase_for_all_outputs pour {city_folder}")
                automate_supabase_for_all_outputs([city_folder])
                inserted_rows.append({'city': city, 'country': country})
            except Exception as e:
                errors.append({'city': city, 'country': country, 'error': str(e)})
        # Stocke le résultat dans la session pour affichage après redirect
        session['inserted_rows'] = inserted_rows
        session['insert_errors'] = errors
    return redirect(url_for('index'))

# WSGI application
application = app

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 