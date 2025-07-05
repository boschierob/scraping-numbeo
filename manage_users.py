import json
import os
import hashlib
import getpass
import questionary

USERS_FILE = os.path.join("datas", "users.json")

def load_users():
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_users(users):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2)

def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def add_user(users):
    username = input("Nouvel identifiant : ").strip()
    if username in users:
        print("Cet identifiant existe déjà.")
        return
    password = getpass.getpass("Nouveau mot de passe : ")
    users[username] = hash_password(password)
    print("Utilisateur ajouté.")

def select_user(users, action_name="sélectionner"):
    user_list = list(users)
    user_list.sort()
    prompt = f"Début de l'identifiant à {action_name} (2 caractères minimum, ou Entrée pour menu) : "
    prefix = input(prompt).strip()
    if prefix == "":
        # Menu interactif avec questionary
        if not user_list:
            print("Aucun utilisateur enregistré.")
            return None
        user_list_menu = user_list + ["<< Annuler >>"]
        answer = questionary.select(
            f"Choisissez l'utilisateur à {action_name} :",
            choices=user_list_menu
        ).ask()
        if answer == "<< Annuler >>" or answer is None:
            print("Action annulée.")
            return None
        return answer
    if len(prefix) < 2:
        print("Veuillez entrer au moins 2 caractères ou appuyer sur Entrée pour le menu.")
        return None
    prefix_lower = prefix.lower()
    startswith_matches = [u for u in users if u.lower().startswith(prefix_lower)]
    if startswith_matches:
        if len(startswith_matches) == 1:
            return startswith_matches[0]
        else:
            print(f"Plusieurs utilisateurs commencent par '{prefix}' :")
            for i, u in enumerate(startswith_matches, 1):
                print(f"{i}. {u}")
            idx = input("Numéro de l'utilisateur à choisir : ").strip()
            if idx.isdigit() and 1 <= int(idx) <= len(startswith_matches):
                return startswith_matches[int(idx)-1]
            else:
                print("Choix invalide.")
                return None
    else:
        contains_matches = [u for u in users if prefix_lower in u.lower()]
        if contains_matches:
            print(f"Aucun identifiant ne commence par '{prefix}', mais voici ceux qui le contiennent :")
            for i, u in enumerate(contains_matches, 1):
                print(f"{i}. {u}")
            idx = input("Numéro de l'utilisateur à choisir : ").strip()
            if idx.isdigit() and 1 <= int(idx) <= len(contains_matches):
                return contains_matches[int(idx)-1]
            else:
                print("Choix invalide.")
                return None
        else:
            print(f"Aucun utilisateur ne correspond à '{prefix}'.")
            return None

def remove_user(users):
    username = select_user(users, action_name="supprimer")
    if not username:
        return
    del users[username]
    print("Utilisateur supprimé.")

def modify_user(users):
    username = select_user(users, action_name="modifier")
    if not username:
        return
    print("1. Modifier l'identifiant")
    print("2. Modifier le mot de passe")
    choice = input("Votre choix : ").strip()
    if choice == "1":
        new_username = input("Nouvel identifiant : ").strip()
        if new_username in users:
            print("Cet identifiant existe déjà.")
            return
        users[new_username] = users.pop(username)
        print("Identifiant modifié.")
    elif choice == "2":
        new_password = getpass.getpass("Nouveau mot de passe : ")
        users[username] = hash_password(new_password)
        print("Mot de passe modifié.")
    else:
        print("Choix invalide.")

def list_users(users):
    if not users:
        print("Aucun utilisateur enregistré.")
    else:
        print("Utilisateurs enregistrés :")
        for username in users:
            print(f"- {username}")

def search_user(users):
    prefix = input("Début de l'identifiant à rechercher (2 caractères minimum) : ").strip()
    if len(prefix) < 2:
        print("Veuillez entrer au moins 2 caractères.")
        return
    prefix_lower = prefix.lower()
    startswith_matches = [u for u in users if u.lower().startswith(prefix_lower)]
    if startswith_matches:
        if len(startswith_matches) == 1:
            print(f"Utilisateur trouvé : {startswith_matches[0]}")
        else:
            print(f"Plusieurs utilisateurs commencent par '{prefix}' :")
            for u in startswith_matches:
                print(f"- {u}")
    else:
        contains_matches = [u for u in users if prefix_lower in u.lower()]
        if contains_matches:
            print(f"Aucun identifiant ne commence par '{prefix}', mais voici ceux qui le contiennent :")
            for u in contains_matches:
                print(f"- {u}")
        else:
            print(f"Aucun utilisateur ne correspond à '{prefix}'.")

def main():
    users = load_users()
    while True:
        print("\nQue voulez-vous faire ?")
        print("1. Ajouter un utilisateur")
        print("2. Supprimer un utilisateur")
        print("3. Modifier un utilisateur")
        print("4. Voir la liste des utilisateurs")
        print("5. Rechercher un utilisateur")
        print("6. Quitter")
        choice = input("Votre choix : ").strip()
        if choice == "1":
            add_user(users)
        elif choice == "2":
            remove_user(users)
        elif choice == "3":
            modify_user(users)
        elif choice == "4":
            list_users(users)
        elif choice == "5":
            search_user(users)
        elif choice == "6":
            save_users(users)
            print("Modifications sauvegardées.")
            break
        else:
            print("Choix invalide.")

if __name__ == "__main__":
    main() 