# Numbeo Scraping Project - Modular Structure

Ce projet a été refactorisé en une structure modulaire pour améliorer la maintenabilité et l'extensibilité.

## Structure du Projet

```
Numbeo-scraping/
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py          # Configuration globale
│   ├── data/
│   │   ├── __init__.py
│   │   └── city_loader.py       # Chargement des données des villes
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── url_builder.py       # Construction des URLs
│   │   └── file_saver.py        # Sauvegarde des fichiers
│   ├── scrapers/
│   │   ├── __init__.py
│   │   └── base_scraper.py      # Classe de base pour les scrapers
│   ├── monitoring/
│   │   ├── __init__.py
│   │   └── stats_tracker.py     # Suivi des statistiques
│   └── __init__.py
├── main.py                      # Point d'entrée principal
├── requirements.txt
└── README.md
```

## Modules

### 1. Configuration (`src/config/`)
- **settings.py** : Tous les paramètres de configuration (URLs, délais, sélecteurs, etc.)

### 2. Données (`src/data/`)
- **city_loader.py** : Chargement et validation des données des villes depuis le CSV

### 3. Utilitaires (`src/utils/`)
- **url_builder.py** : Construction des URLs pour les différentes catégories
- **file_saver.py** : Sauvegarde des données en CSV et Excel

### 4. Scrapers (`src/scrapers/`)
- **base_scraper.py** : Classe de base avec fonctionnalités communes (requêtes HTTP, parsing HTML, etc.)

### 5. Monitoring (`src/monitoring/`)
- **stats_tracker.py** : Suivi des statistiques et génération de rapports

## Utilisation

### Installation
```bash
pip install -r requirements.txt
```

### Exécution
```bash
python main.py
```

## Fonctionnalités Implémentées

### ✅ Structure de base modulaire
- Séparation claire des responsabilités
- Configuration centralisée
- Gestion des erreurs et logging
- Suivi des statistiques

### ✅ Chargement des données
- Lecture du fichier CSV des villes
- Validation des URLs
- Gestion des erreurs de chargement

### ✅ Construction des URLs
- Génération automatique des URLs pour toutes les catégories
- Validation des URLs
- Extraction des identifiants de ville

### ✅ Sauvegarde des fichiers
- Sauvegarde en CSV et Excel
- Organisation par catégories
- Noms de fichiers sécurisés
- Gestion des dossiers de sortie

### ✅ Monitoring et rapports
- Suivi détaillé des statistiques
- Génération de rapports JSON et texte
- Calcul des taux de succès
- Gestion des erreurs

## Prochaines Étapes

### 🔄 À implémenter
1. **Scrapers spécifiques par catégorie** :
   - Quality of Life scraper
   - Crime scraper
   - Cost of Living scraper
   - Health Care scraper
   - Climate scraper
   - Property Investment scraper
   - Traffic scraper
   - Pollution scraper

2. **Intégration du scraping réel** dans `main.py`

3. **Script d'import MySQL** conditionnel

4. **Tests unitaires** pour chaque module

## Avantages de cette Structure

1. **Maintenabilité** : Code organisé et facile à maintenir
2. **Extensibilité** : Facile d'ajouter de nouvelles catégories ou fonctionnalités
3. **Testabilité** : Chaque module peut être testé indépendamment
4. **Réutilisabilité** : Composants réutilisables dans d'autres projets
5. **Débogage** : Logging et monitoring détaillés

## Configuration

Tous les paramètres sont centralisés dans `src/config/settings.py` :
- URLs de base
- Délais entre requêtes
- Sélecteurs de tables
- Paramètres de base de données
- Configuration du logging

## Logs et Rapports

Le système génère automatiquement :
- Logs détaillés dans `logs/scraping.log`
- Rapports de session dans le dossier de sortie
- Statistiques de succès/échec
- Liste des erreurs rencontrées 