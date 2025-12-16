"""Module d'enrichissement des données (adapté Météo)."""
import pandas as pd
from typing import Optional, List
from tqdm import tqdm

from .fetchers.adresse import AdresseFetcher
from .models import WeatherForecast, GeocodingResult


class DataEnricher:
    """Enrichit les données en croisant plusieurs sources."""
    
    def __init__(self):
        self.geocoder = AdresseFetcher()
        self.enrichment_stats = {
            "total_processed": 0,
            "successfully_enriched": 0,
            "failed_enrichment": 0,
        }
    
    def extract_addresses(self, forecasts: list[dict], address_field: str = "original_city_name") -> List[str]:
        """
        Extrait les noms de villes uniques à géocoder.
        """
        addresses = set()
        
        for forecast in forecasts:
            addr = forecast.get(address_field, "")
            if addr and isinstance(addr, str) and addr.strip():
                addresses.add(addr.strip())
        
        return list(addresses)
    
    # build_geocoding_cache reste inchangé
    def build_geocoding_cache(self, addresses: list[str]) -> dict[str, GeocodingResult]:
        """Construit un cache de géocodage pour éviter les requêtes en double."""
        cache = {}
        print(f"🌍 Géocodage de {len(addresses)} adresses/villes uniques...")
        for result in self.geocoder.fetch_all(addresses):
            cache[result.original_address] = result
        
        success_rate = sum(1 for r in cache.values() if r.is_valid) / len(cache) * 100 if cache else 0
        print(f"✅ Taux de succès: {success_rate:.1f}%")
        return cache
    
    
    def enrich_forecasts(
        self, 
        forecasts: list[dict], 
        geocoding_cache: dict[str, GeocodingResult],
        city_name_field: str = "original_city_name"
    ) -> list[dict]:
        """
        Enrichit les prévisions météo avec les données de géocodage de l'API Adresse.
        """
        enriched = []
        
        for forecast in tqdm(forecasts, desc="Enrichissement"):
            self.enrichment_stats["total_processed"] += 1
            
            enriched_forecast = forecast.copy()
            city_name = forecast.get(city_name_field, "")
            
            if city_name in geocoding_cache:
                geo = geocoding_cache[city_name]
                
                # Mise à jour des champs d'enrichissement
                enriched_forecast["validated_city"] = geo.city
                enriched_forecast["validated_postal_code"] = geo.postal_code
                enriched_forecast["geocoding_score"] = geo.score
                
                # On met à jour les coordonnées avec les coordonnées validées si le score est bon
                if geo.is_valid:
                    enriched_forecast["latitude"] = geo.latitude
                    enriched_forecast["longitude"] = geo.longitude
                    self.enrichment_stats["successfully_enriched"] += 1
                else:
                    self.enrichment_stats["failed_enrichment"] += 1
            
            enriched.append(enriched_forecast)
        
        return enriched
    
    def get_stats(self) -> dict:
        """Retourne les statistiques d'enrichissement."""
        stats = self.enrichment_stats.copy()
        stats["geocoder_stats"] = self.geocoder.get_stats()
        
        if stats["total_processed"] > 0:
            stats["success_rate"] = stats["successfully_enriched"] / stats["total_processed"] * 100
        else:
            stats["success_rate"] = 0
        
        return stats