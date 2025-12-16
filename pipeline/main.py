#!/usr/bin/env python3
"""Script principal du pipeline."""
import argparse
from datetime import datetime
import pandas as pd
import logging # NOUVEAU
from logging.handlers import RotatingFileHandler # NOUVEAU (pour le log structuré)
from pathlib import Path # NOUVEAU (pour les chemins de log)

from .fetchers.openmeteo import OpenMeteoFetcher, CITIES_FRANCE 
from .enricher import DataEnricher
from .transformer import DataTransformer
from .quality import QualityAnalyzer
# Import des fonctions de stockage + la nouvelle classe StorageManager (à importer)
from .storage import save_raw_json, save_parquet, StorageManager 
from .config import MAX_ITEMS, REPORTS_DIR # Import REPORTS_DIR pour le check incrémental

# Configuration du logger
logger = logging.getLogger(__name__) # NOUVEAU

def setup_logging():
    """Configure le système de logging structuré (Bonus)."""
    log_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configuration du Handler pour la console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    # Configuration du Handler pour le fichier (Rolling file)
    log_file = Path('logs/pipeline.log')
    log_file.parent.mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=1024*1024*5, # 5MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # Niveau par défaut
    
    # Évite d'ajouter plusieurs fois les handlers lors des rechargements (si besoin)
    if not root_logger.handlers:
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)


def run_pipeline(
    max_items: int = MAX_ITEMS,
    skip_enrichment: bool = False,
    verbose: bool = True
) -> dict:
    """
    Exécute le pipeline complet.
    
    Args:
        max_items: Nombre max de villes à interroger
        skip_enrichment: Passer l'enrichissement (plus rapide)
        verbose: Afficher la progression
    
    Returns:
        Statistiques du pipeline
    """
    stats = {"start_time": datetime.now()}
    
    storage_manager = StorageManager() # NOUVEAU : Instancier le manager
    
    logger.info("=" * 60)
    logger.info("🚀 PIPELINE OPEN DATA - MÉTÉO & GÉO")
    logger.info("=" * 60)
    
    # --- LOGIQUE INCRÉMENTALE (BONUS) ---
    if storage_manager.file_exists_for_today(REPORTS_DIR, "meteo_quality"):
        logger.warning(f"Pipeline sauté : Rapport de qualité déjà existant pour aujourd'hui dans {REPORTS_DIR}. Exécution incrémentale.")
        return {"status": "skipped_incremental", "end_time": datetime.now()}
    
    # === ÉTAPE 1 : Acquisition ===
    city_list = CITIES_FRANCE 
    
    logger.info("\n📥 ÉTAPE 1 : Acquisition des données") # LOG CHANGE
    fetcher = OpenMeteoFetcher()
    forecasts = list(fetcher.fetch_all(city_list, max_items, verbose)) 
    
    if not forecasts:
        logger.error("❌ Aucune prévision récupérée. Arrêt.") # LOG CHANGE
        return {"error": "No data fetched"}
    
    save_raw_json(forecasts, "meteo_raw")
    stats["fetcher"] = fetcher.get_stats()
    
    # === ÉTAPE 2 : Enrichissement ===
    if not skip_enrichment:
        logger.info("\n🌍 ÉTAPE 2 : Enrichissement (géocodage)") # LOG CHANGE
        enricher = DataEnricher()
        
        # Extraire les noms de villes uniques
        addresses = enricher.extract_addresses(forecasts, "original_city_name")
        
        if addresses:
            # Construire le cache de géocodage
            geo_cache = enricher.build_geocoding_cache(addresses) 
            
            # Enrichir les prévisions
            forecasts = enricher.enrich_forecasts(forecasts, geo_cache, "original_city_name") 
            stats["enricher"] = enricher.get_stats()
        else:
            logger.warning("⚠️ Pas de villes à géocoder") # LOG CHANGE
    else:
        logger.info("\n⏭️ ÉTAPE 2 : Enrichissement (ignoré)") # LOG CHANGE
    
    # === ÉTAPE 3 : Transformation ===
    logger.info("\n🔧 ÉTAPE 3 : Transformation et nettoyage") # LOG CHANGE
    df = pd.DataFrame(forecasts)
    
    transformer = DataTransformer(df)
    df_clean = (
        transformer
        .remove_duplicates(['date', 'latitude', 'longitude']) 
        .handle_missing_values(numeric_strategy='median', text_strategy='unknown')
        .normalize_text_columns(['validated_city'])
        .add_derived_columns()
        .get_result()
    )
    
    logger.info(f"   Résumé des transformations:\n{transformer.get_summary()}") # LOG CHANGE
    stats["transformer"] = {"transformations": transformer.transformations_applied}
    
    # === ÉTAPE 4 : Qualité ===
    logger.info("\n📊 ÉTAPE 4 : Analyse de qualité") # LOG CHANGE
    analyzer = QualityAnalyzer(df_clean)
    metrics = analyzer.analyze()
    
    logger.info(f"   Note: {metrics.quality_grade}") # LOG CHANGE
    logger.info(f"   Complétude: {metrics.completeness_score * 100:.1f}%") # LOG CHANGE
    logger.info(f"   Doublons: {metrics.duplicates_pct:.1f}%") # LOG CHANGE
    
    # Générer le rapport
    analyzer.generate_report("meteo_quality")
    stats["quality"] = metrics.dict()
    
    # === ÉTAPE 5 : Stockage ===
    logger.info("\n💾 ÉTAPE 5 : Stockage final") # LOG CHANGE
    output_path = save_parquet(df_clean, "meteo_enriched") 
    stats["output_path"] = str(output_path)
    
    # === RÉSUMÉ ===
    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).seconds
    
    logger.info("\n" + "=" * 60) # LOG CHANGE
    logger.info("✅ PIPELINE TERMINÉ") # LOG CHANGE
    logger.info("=" * 60) # LOG CHANGE
    logger.info(f"   Durée: {stats['duration_seconds']}s") # LOG CHANGE
    logger.info(f"   Enregistrements: {len(df_clean)}") # LOG CHANGE
    logger.info(f"   Qualité: {metrics.quality_grade}") # LOG CHANGE
    logger.info(f"   Fichier: {output_path}") # LOG CHANGE
    
    return stats


def main():
    # NOUVEAU : Configure le logging en premier
    setup_logging()
    
    parser = argparse.ArgumentParser(description="Pipeline Open Data Météo")
    parser.add_argument("--max-items", "-m", type=int, default=MAX_ITEMS, help="Nombre max de villes")
    parser.add_argument("--skip-enrichment", "-s", action="store_true", help="Ignorer l'enrichissement")
    parser.add_argument("--verbose", "-v", action="store_true", default=True)
    
    args = parser.parse_args()
    
    run_pipeline(
        max_items=args.max_items,
        skip_enrichment=args.skip_enrichment,
        verbose=args.verbose
    )


if __name__ == "__main__":
    main()