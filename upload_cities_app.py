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

st.title("Uploader un fichier CSV de villes")

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
            st.experimental_rerun()
        else:
            st.error("Identifiant ou mot de passe incorrect.")
else:
    uploaded_file = st.file_uploader("Choisissez un fichier CSV", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Aperçu du fichier :", df.head())
        expected_cols = {"city", "country"}
        save_path = os.path.join("datas", "cities.csv")
        if set(df.columns) == expected_cols:
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
            st.error(f"Le fichier doit contenir exactement les colonnes : {expected_cols}. Colonnes trouvées : {list(df.columns)}")
    if st.button("Se déconnecter"):
        st.session_state.logged_in = False
        st.experimental_rerun() 