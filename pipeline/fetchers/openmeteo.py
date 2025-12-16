"""Fetcher pour l'API OpenMeteo."""
from typing import Generator, List
from tqdm import tqdm
from datetime import datetime, timedelta

from .base import BaseFetcher
from ..config import OPENMETEO_CONFIG, MAX_ITEMS, BATCH_SIZE
from ..models import WeatherForecast

# Simuler une liste de villes françaises pour l'acquisition
# En production, cette liste viendrait d'une autre base de données.
CITIES_FRANCE = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", 
    "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", 
    "Reims", "Le Havre", "Saint-Étienne", "Toulon", "Grenoble", 
    "Dijon", "Angers", "Villeurbanne", "Saint-Denis", "Nîmes",
    "Clermont-Ferrand", "Le Mans", "Aix-en-Provence", "Brest", 
    "Tours", "Limoges", "Amiens", "Perpignan", "Metz", "Besançon",
    "Orléans", "Rouen", "Mulhouse", "Caen", "Nancy", "Argenteuil", 
    "Saint-Paul", "Montreuil", "Saint-Denis", "Tourcoing", "Avignon", 
    "Poitiers", "Versailles", "Nanterre", "Courbevoie", "Rueil-Malmaison",
    "Pau", "Calais", "La Rochelle", "Antibes"
]


class OpenMeteoFetcher(BaseFetcher):
    """Fetcher pour OpenMeteo."""
    
    def __init__(self):
        super().__init__(OPENMETEO_CONFIG)
    
    def fetch_weather_by_coords(self, lat: float, lon: float) -> dict:
        """Récupère la prévision pour 7 jours pour des coordonnées spécifiques."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code",
            "forecast_days": 7,
            "timezone": "Europe/Paris"
        }
        
        try:
            return self._make_request("/forecast", params)
        except Exception as e:
            self.stats["requests_failed"] += 1
            print(f"⚠️ Erreur météo pour {lat},{lon}: {e}")
            return {}

    def fetch_batch(self, **kwargs) -> list[dict]:
        """
        Implémentation minimale requise par la classe abstraite BaseFetcher.
        Retourne une liste vide car la logique est gérée par fetch_all.
        """
        return []



    def fetch_all(
        self, 
        city_names: List[str] = None,
        max_items: int = MAX_ITEMS,
        verbose: bool = True
    ) -> Generator[WeatherForecast, None, None]:
        """
        Récupère les prévisions pour une liste de villes.
        NOTE: Les coordonnées (latitude/longitude) doivent être fournies avant l'appel.
        Dans ce TP, nous utilisons des coordonnées fixes pour simuler l'acquisition initiale.
        Le géocodage réel sera fait dans l'enrichisseur.
        """
        if city_names is None:
            city_names = CITIES_FRANCE
            
        from datetime import datetime
        
        self.stats["start_time"] = datetime.now()
        total_fetched = 0
        
        # Utiliser uniquement le nombre max de villes requis
        cities_to_fetch = city_names[:max_items]
        
        # Simuler les coordonnées (sera corrigé par l'enrichisseur)
        DUMMY_COORDS = {
            "Paris": (48.8566, 2.3522),
            "Lyon": (45.7578, 4.8320),
            "Marseille": (43.2965, 5.3698),
            # ... (Ajoutez d'autres si nécessaire, sinon OpenMeteo les prendra)
        }

        pbar = tqdm(cities_to_fetch, desc="OpenMeteo Acquisition", disable=not verbose)
        
        for city in pbar:
            # Tente d'utiliser les coordonnées simulées ou prend celles de Paris
            lat, lon = DUMMY_COORDS.get(city, (48.8566, 2.3522))
            
            data = self.fetch_weather_by_coords(lat, lon)
            
            if not data or not data.get('daily'):
                continue
            
            # Traiter les 7 jours de prévision
            daily_data = data['daily']
            
            for i in range(len(daily_data['time'])):
                forecast_date = datetime.strptime(daily_data['time'][i], '%Y-%m-%d')
                
                # Construire le modèle WeatherForecast
                try:
                    forecast = WeatherForecast(
                        date=forecast_date,
                        latitude=data['latitude'],
                        longitude=data['longitude'],
                        temperature_max=daily_data['temperature_2m_max'][i],
                        temperature_min=daily_data['temperature_2m_min'][i],
                        precipitation_sum=daily_data['precipitation_sum'][i],
                        weather_code=daily_data['weather_code'][i],
                        original_city_name=city, # La colonne clé pour l'enrichissement
                    )
                    
                    yield forecast.dict()
                    total_fetched += 1
                    self.stats["items_fetched"] += 1
                    
                except Exception as e:
                    print(f"Erreur de validation Pydantic pour {city} à J+{i}: {e}")
                    continue
            
            # Rate limiting
            self._rate_limit()
        
        pbar.close()
        self.stats["end_time"] = datetime.now()
        
        if verbose:
            duration = (self.stats["end_time"] - self.stats["start_time"]).seconds
            print(f"✅ {total_fetched} prévisions (jours x villes) récupérées en {duration}s")