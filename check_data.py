# check_data.py

import duckdb

# Nom du fichier Parquet généré par la dernière exécution
FILE_PATH = "data/processed/meteo_enriched_20251216_161514.parquet"

# Connexion à DuckDB
conn = duckdb.connect()

print("--- Inspection du Fichier Parquet ---")

try:
    # 1. Compter les enregistrements
    count_query = f"SELECT COUNT(*) FROM '{FILE_PATH}'"
    count = conn.execute(count_query).fetchall()[0][0]
    print(f"Nombre total d'enregistrements: {count}")
    
    # 2. Afficher un aperçu
    sample_query = f"SELECT * FROM '{FILE_PATH}' LIMIT 3"
    print("\nTrois premiers enregistrements:")
    results = conn.execute(sample_query).fetchdf()
    print(results.T) # Afficher en format transposable pour une meilleure lisibilité dans la console

except Exception as e:
    print(f"❌ Erreur lors de la lecture du fichier Parquet: {e}")
    print("Vérifiez que le chemin du fichier est correct.")
    
finally:
    conn.close()