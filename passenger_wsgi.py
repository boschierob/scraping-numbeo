#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import threading
import time
from pathlib import Path

# Ajouter le répertoire du projet au PYTHONPATH
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Application WSGI simple qui redirige vers Streamlit
def application(environ, start_response):
    """Application WSGI qui redirige vers Streamlit"""
    
    # Redirection vers Streamlit
    status = '302 Found'
    headers = [
        ('Location', 'http://127.0.0.1:8501'),
        ('Content-Type', 'text/html'),
    ]
    start_response(status, headers)
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Redirection vers Numbeo Scraping</title>
        <meta http-equiv="refresh" content="0; url=http://127.0.0.1:8501">
    </head>
    <body>
        <p>Redirection vers l'application Streamlit...</p>
        <p><a href="http://127.0.0.1:8501">Cliquez ici si la redirection ne fonctionne pas</a></p>
    </body>
    </html>
    """
    
    return [html_content.encode('utf-8')]

# Point d'entrée pour les tests
if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    print("Démarrage du serveur WSGI...")
    httpd = make_server('', 8000, application)
    print("Serveur WSGI démarré sur le port 8000")
    httpd.serve_forever() 