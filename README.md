# 🚀 Pipeline Data Engineering : Météo & Géodonnées

![Python Version](https://img.shields.io/badge/python-3.12-blue)
![Status](https://img.shields.io/badge/status-production-green)
![Data Quality](https://img.shields.io/badge/quality-grade%20A-brightgreen)

## 📋 Description

Ce projet implémente un pipeline de données **ETL (Extract, Transform, Load)** robuste et résilient qui agrège, nettoie et enrichit des données météorologiques et géographiques.

Le pipeline est conçu pour être **tolérant aux pannes** et inclut un module d'analyse de qualité avancé utilisant l'Intelligence Artificielle (Gemini/Ollama) pour générer des rapports automatisés.

### 🌟 Fonctionnalités Clés

* **Acquisition Multi-sources :**
    * Météo : [Open-Meteo API](https://open-meteo.com/) (Prévisions à 7 jours).
    * Géographie : [API Adresse Data.gouv](https://adresse.data.gouv.fr/) (Géocodage et normalisation).
* **Architecture Résiliente :**
    * Système de **Retry exponentiel** pour les appels API (gestion des erreurs 429/50x).
    * **Fallback IA** : Bascule automatique de Google Gemini vers Ollama (local) en cas de panne ou quota dépassé.
    * **Exécution Incrémentale** : Détection intelligente des traitements déjà effectués pour éviter la redondance.
* **Qualité des Données :**
    * Calcul automatique de scores (Complétude, Doublons, Validité).
    * Génération de rapports d'audit en Markdown via LLM.
    * Nettoyage et déduplication intelligente post-enrichissement.
* **Stockage Optimisé :**
    * Données brutes en JSON (Auditabilité).
    * Données traitées en Parquet (Performance analytique).
* **Monitoring :**
    * Logs structurés (Console + Fichier rotatif).

---

## 🏗️ Architecture du Pipeline

Le pipeline suit une architecture séquentielle modulaire :

1.  **📥 Acquisition (`Fetchers`)** : Récupération des prévisions météo pour une liste de villes cibles.
2.  **🌍 Enrichissement (`Enricher`)** : Correction des coordonnées et normalisation des noms de villes via géocodage.
3.  **🔧 Transformation (`Transformer`)** : Nettoyage, typage, suppression des doublons et création de colonnes dérivées (ex: amplitude thermique).
4.  **📊 Qualité (`QualityAnalyzer`)** : Audit statistique et analyse sémantique par IA.
5.  **💾 Stockage (`Storage`)** : Sauvegarde des artefacts finaux.

---

## 🛠️ Installation

### Prérequis

* Python 3.10+
* [UV](https://github.com/astral-sh/uv) (Recommandé) ou Pip.
* (Optionnel) [Ollama](https://ollama.com/) installé localement pour le mode offline/fallback.

### Configuration

1.  **Cloner le dépôt :**
    ```bash
    git clone [https://github.com/votre-user/cours_open_data_tp2.git](https://github.com/votre-user/cours_open_data_tp2.git)
    cd cours_open_data_tp2
    ```

2.  **Installer les dépendances :**
    
    *Via UV :*
    ```bash
    uv sync
    ```
    *Via Pip (Standard) :*
    ```bash
    pip install -r requirements.txt
    ```

3.  **Variables d'environnement (.env) :**
    Créez un fichier `.env` à la racine :
    ```ini
    # Clé API Google Gemini (Optionnel, le pipeline basculera sur Ollama si absent)
    GEMINI_API_KEY=votre_cle_ici

    # Configuration Ollama (si utilisé en fallback)
    OLLAMA_BASE_URL=http://localhost:11434
    ```

---

## 🚀 Utilisation

### Lancer le Pipeline

Pour exécuter le pipeline complet (Acquisition -> Stockage) :

**Avec UV :**
```bash
uv run python -m pipeline.main
```

**Avec Python standard :**
```bash
# Assurez-vous d'avoir activé votre environnement virtuel
python -m pipeline.main
```

**Options Disponibles**
Le pipeline accepte plusieurs arguments pour personnaliser l'exécution :

| Option | Raccourci | Description |
| :--- | :--- | :--- |
| `--max-items` | `-m` | Limiter le nombre de villes à traiter (pour les tests) |
| `--skip-enrichment` | `-s` | Sauter l'étape de géocodage |
| `--verbose` | `-v` | Afficher plus de détails dans la console |


**Exemples :**
```bash
# Avec UV - Traiter 10 villes en mode verbose
uv run python -m pipeline.main -m 10 -v

# Avec Python standard - Traiter 10 villes en mode verbose
python -m pipeline.main -m 10 -v

# Sauter l'enrichissement géographique
python -m pipeline.main -s
```


**Vérifier les Données**
Un script utilitaire est fourni pour inspecter rapidement le fichier Parquet généré :
```bash
# Avec UV
uv run python check_data.py

# Avec Python standard
python check_data.py
```


## 📂 Structure du Projet
```text
📁 cours_open_data_tp2
├── 📁 data/                  # Stockage des données (ignoré par Git)
│   ├── raw/                  # JSON bruts
│   ├── processed/            # Parquet finaux
│   └── reports/              # Rapports de qualité Markdown
├── 📁 logs/                  # Fichiers de logs rotatifs
├── 📁 pipeline/              # Code source du pipeline
│   ├── 📁 fetchers/          # Modules d'acquisition API
│   ├── config.py             # Configuration centralisée
│   ├── enricher.py           # Logique de géocodage
│   ├── main.py               # Point d'entrée et orchestration
│   ├── models.py             # Schémas de données Pydantic
│   ├── quality.py            # Moteur de qualité et IA
│   ├── storage.py            # Gestion I/O
│   └── transformer.py        # Logique de nettoyage Pandas
├── 📁 tests/                 # Tests unitaires (Pytest)
├── .env                      # Secrets (non versionné)
├── check_data.py             # Script d'inspection
├── pyproject.toml            # Dépendances et configuration
└── README.md                 # Ce fichier
```

## 🧪 Tests
Le projet inclut une suite de tests unitaires pour garantir la fiabilité des transformations et des connexions API.
Lancer les tests :
```bash
# Avec UV
uv run python -m pytest test/ -v

# Avec Python standard
pytest
```

## 📊 Rapport de Qualité
Le pipeline génère automatiquement un rapport de qualité dans `data/reports/meteo_quality_YYYYMMDD.md`.
```text
Exemple de contenu :
markdownNote de Qualité : A

Métriques :
- Complétude : 100.0%
- Doublons : 0.0%

Recommandations IA :
1. Les données sont propres et prêtes pour l'analyse.
2. La couverture géographique est cohérente.
...
```

## 🔐 Configuration
Créez un fichier `.env` à la racine du projet avec vos clés API :
```env
API_KEY_METEO=votre_cle_api
API_KEY_GEOCODING=votre_cle_api
```

## 👤 Auteur
Projet réalisé dans le cadre du cours Open Data (TP2).