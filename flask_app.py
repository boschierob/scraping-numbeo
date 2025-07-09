#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, render_template_string, request, redirect, flash, session
import os
import sys
import json
import hashlib
from pathlib import Path

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
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Numbeo Scraping - {% block title %}{% endblock %}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        .form-group { margin: 20px 0; }
        input, textarea, button { padding: 10px; margin: 5px; }
        .success { color: green; }
        .error { color: red; }
        .warning { color: orange; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Numbeo Scraping</h1>
        {% with messages = get_flashed_messages(with_categories=true) %}
          {% if messages %}
            <ul>
            {% for category, message in messages %}
              <li class="{{ category }}">{{ message }}</li>
            {% endfor %}
            </ul>
          {% endif %}
        {% endwith %}
        {% block content %}{% endblock %}
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, title="Accueil") + """
    <h2>Scraper des villes Numbeo par URL</h2>
    
    {% if not session.get('logged_in') %}
        <h3>Connexion requise</h3>
        <form method="POST" action="/login">
            <div class="form-group">
                <label>Identifiant:</label><br>
                <input type="text" name="username" required>
            </div>
            <div class="form-group">
                <label>Mot de passe:</label><br>
                <input type="password" name="password" required>
            </div>
            <button type="submit">Se connecter</button>
        </form>
    {% else %}
        <p class="success">Connecté en tant que {{ session.get('username') }}</p>
        
        <h3>Scraping d'URLs Numbeo</h3>
        <form method="POST" action="/scrape_urls">
            <div class="form-group">
                <label>URLs Numbeo (une par ligne):</label><br>
                <textarea name="urls" rows="5" cols="50" placeholder="https://www.numbeo.com/cost-of-living/in/Lyon"></textarea>
            </div>
            <button type="submit">Lancer le scraping</button>
        </form>
        
        <form method="POST" action="/logout">
            <button type="submit">Se déconnecter</button>
        </form>
    {% endif %}
    """

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

    # Appel du scraping pour chaque URL
    from main import scrape_from_url  # Import ici pour éviter les problèmes de dépendances circulaires
    for url in [u.strip() for u in urls.splitlines() if u.strip()]:
        try:
            scrape_from_url(url)
            flash(f"✅ Succès pour {url}", "success")
        except Exception as e:
            flash(f"❌ Erreur pour {url} : {e}", "error")
    return redirect('/')

# WSGI application
application = app

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 