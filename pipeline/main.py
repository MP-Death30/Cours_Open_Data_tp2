#!/usr/bin/env python3
"""Script principal du pipeline."""
import argparse
from datetime import datetime
import pandas as pd

from .fetchers.openmeteo import OpenMeteoFetcher, CITIES_FRANCE # Utiliser le nouveau fetcher
from .enricher import DataEnricher
from .transformer import DataTransformer
from .quality import QualityAnalyzer
from .storage import save_raw_json, save_parquet
from .config import MAX_ITEMS


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
    
    print("=" * 60)
    print("🚀 PIPELINE OPEN DATA - MÉTÉO & GÉO")
    print("=" * 60)
    
    # === ÉTAPE 1 : Acquisition ===
    # Ici, la "catégorie" est la liste de villes à interroger
    city_list = CITIES_FRANCE 
    
    print("\n📥 ÉTAPE 1 : Acquisition des données")
    fetcher = OpenMeteoFetcher()
    # Récupère les prévisions pour MAX_ITEMS villes
    forecasts = list(fetcher.fetch_all(city_list, max_items, verbose)) 
    
    if not forecasts:
        print("❌ Aucune prévision récupérée. Arrêt.")
        return {"error": "No data fetched"}
    
    save_raw_json(forecasts, "meteo_raw")
    stats["fetcher"] = fetcher.get_stats()
    
    # === ÉTAPE 2 : Enrichissement ===
    if not skip_enrichment:
        print("\n🌍 ÉTAPE 2 : Enrichissement (géocodage)")
        enricher = DataEnricher()
        
        # Extraire les noms de villes uniques
        addresses = enricher.extract_addresses(forecasts, "original_city_name")
        
        if addresses:
            # Construire le cache de géocodage
            # On utilise toutes les adresses, car MAX_ITEMS est déjà bas
            geo_cache = enricher.build_geocoding_cache(addresses) 
            
            # Enrichir les prévisions
            # Utiliser la méthode adaptée pour les prévisions
            forecasts = enricher.enrich_forecasts(forecasts, geo_cache, "original_city_name") 
            stats["enricher"] = enricher.get_stats()
        else:
            print("⚠️ Pas de villes à géocoder")
    else:
        print("\n⏭️ ÉTAPE 2 : Enrichissement (ignoré)")
    
    # === ÉTAPE 3 : Transformation ===
    print("\n🔧 ÉTAPE 3 : Transformation et nettoyage")
    df = pd.DataFrame(forecasts)
    
    transformer = DataTransformer(df)
    # Les transformations sont génériques, mais on pourrait ajouter le nettoyage des codes WMO
    df_clean = (
        transformer
        .remove_duplicates(['date', 'latitude', 'longitude']) 
        .handle_missing_values(numeric_strategy='median', text_strategy='unknown')
        .normalize_text_columns(['validated_city']) # Utiliser validated_city pour la normalisation
        .add_derived_columns()
        .get_result()
    )
    
    print(f"   Résumé des transformations:\n{transformer.get_summary()}")
    stats["transformer"] = {"transformations": transformer.transformations_applied}
    
    # === ÉTAPE 4 : Qualité ===
    print("\n📊 ÉTAPE 4 : Analyse de qualité")
    analyzer = QualityAnalyzer(df_clean)
    metrics = analyzer.analyze()
    
    print(f"   Note: {metrics.quality_grade}")
    print(f"   Complétude: {metrics.completeness_score * 100:.1f}%")
    print(f"   Doublons: {metrics.duplicates_pct:.1f}%")
    
    # Générer le rapport
    analyzer.generate_report("meteo_quality")
    stats["quality"] = metrics.dict()
    
    # === ÉTAPE 5 : Stockage ===
    print("\n💾 ÉTAPE 5 : Stockage final")
    # Nom du fichier par défaut
    output_path = save_parquet(df_clean, "meteo_enriched") 
    stats["output_path"] = str(output_path)
    
    # === RÉSUMÉ ===
    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).seconds
    
    print("\n" + "=" * 60)
    print("✅ PIPELINE TERMINÉ")
    print("=" * 60)
    print(f"   Durée: {stats['duration_seconds']}s")
    print(f"   Enregistrements: {len(df_clean)}")
    print(f"   Qualité: {metrics.quality_grade}")
    print(f"   Fichier: {output_path}")
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Pipeline Open Data Météo")
    # Nous avons retiré l'argument 'category' car la liste est fixe (CITIES_FRANCE)
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