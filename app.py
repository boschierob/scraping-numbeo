#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path

# Ajouter le répertoire du projet au PYTHONPATH
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configuration pour o2switch
os.environ['STREAMLIT_SERVER_PORT'] = '8501'
os.environ['STREAMLIT_SERVER_ADDRESS'] = '0.0.0.0'
os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
os.environ['STREAMLIT_SERVER_ENABLE_CORS'] = 'false'
os.environ['STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION'] = 'false'

# Importer et lancer l'application Streamlit
from streamlit.web.cli import main as streamlit_main

def application(environ, start_response):
    """WSGI application pour Streamlit"""
    # Rediriger vers l'application Streamlit
    status = '200 OK'
    response_headers = [('Content-type', 'text/html')]
    start_response(status, response_headers)
    
    # Lancer Streamlit
    sys.argv = ['streamlit', 'run', 'upload_cities_app.py', '--server.port=8501']
    streamlit_main()
    
    return [b'Streamlit application started']

# Point d'entrée direct pour les tests
if __name__ == '__main__':
    from streamlit.web.cli import main
    sys.argv = ['streamlit', 'run', 'upload_cities_app.py']
    main() 