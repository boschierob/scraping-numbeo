import streamlit as st
import pandas as pd
import os
import json
import hashlib

USERS_FILE = os.path.join("datas", "users.json")

def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def check_login(username, password):
    if not os.path.exists(USERS_FILE):
        return False
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        users = json.load(f)
    return username in users and users[username] == hash_password(password)

st.header("Uploader un fichier CSV de villes")

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.subheader("Connexion requise")
    username = st.text_input("Identifiant")
    password = st.text_input("Mot de passe", type="password")
    if st.button("Se connecter"):
        if check_login(username, password):
            st.session_state.logged_in = True
            st.success("Connexion réussie !")
            st.rerun()
        else:
            st.error("Identifiant ou mot de passe incorrect.")
else:
    uploaded_file = st.file_uploader("Choisissez un fichier CSV", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Aperçu du fichier :", df.head())
        expected_first_cols = ["city", "country", "region"]
        save_path = os.path.join("datas", "cities.csv")
        if list(df.columns[:3]) == expected_first_cols:
            if os.path.exists(save_path):
                if st.button("Écraser le fichier existant ?"):
                    df.to_csv(save_path, index=False)
                    st.success(f"Fichier sauvegardé dans {save_path}")
                else:
                    st.warning("Un fichier cities.csv existe déjà. Cliquez sur le bouton ci-dessus pour l'écraser.")
            else:
                df.to_csv(save_path, index=False)
                st.success(f"Fichier sauvegardé dans {save_path}")
        else:
            st.error(f"Le fichier doit avoir 'city', 'country', puis 'region' comme trois premières colonnes. Colonnes trouvées : {list(df.columns)}")

    # --- Nouvelle section : Scraper à partir d'URLs Numbeo ---
    st.markdown("--- OU ---")
    st.header("Scraper directement à partir d'une ou plusieurs URLs Numbeo")
    st.caption("le systeme extraiera les villes depuis les urls que vous entrerez, puis scrapera toutes les données (coût de la vie, climat, sécurité, ...) relatives à celles-ci")
    urls_input = st.text_area("Collez une ou plusieurs URLs Numbeo (une par ligne)")
    if st.button("Lancer le scraping des URLs"):
        if not urls_input.strip():
            st.warning("Veuillez entrer au moins une URL.")
        else:
            # Import dynamique pour éviter les problèmes de dépendances circulaires
            import sys
            sys.path.insert(0, os.path.abspath("."))
            try:
                from main import scrape_from_url
                urls = [u.strip() for u in urls_input.strip().splitlines() if u.strip()]
                results = []
                for url in urls:
                    st.info(f"🔎 Scraping des infos la ville {url} ...")
                    try:
                        scrape_from_url(url)
                        results.append((url, True, None))
                        st.success(f"✅ Succès pour {url}")
                    except Exception as e:
                        results.append((url, False, str(e)))
                        st.error(f"❌ Erreur pour {url} : {e}")
            except ImportError as e:
                st.error(f"Erreur d'import de la fonction de scraping : {e}")
            except Exception as e:
                st.error(f"Erreur inattendue : {e}")

    if st.button("Se déconnecter"):
        st.session_state.logged_in = False
        st.rerun() 