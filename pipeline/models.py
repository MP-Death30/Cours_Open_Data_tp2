"""Modèles de données avec validation."""
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime


# --- Modèle WeatherForecast (Le modèle principal du dataset) ---
class WeatherForecast(BaseModel):
    """Modèle d'une prévision météo enrichie."""
    
    # Données d'acquisition (OpenMeteo)
    date: datetime # La date de la prévision
    latitude: float
    longitude: float
    temperature_max: Optional[float] = None
    temperature_min: Optional[float] = None
    precipitation_sum: Optional[float] = None
    weather_code: Optional[int] = None # Code WMO
    
    # Données d'enrichissement (API Adresse)
    original_city_name: str # Nom de la ville utilisé pour la requête
    validated_city: Optional[str] = None
    validated_postal_code: Optional[str] = None
    geocoding_score: Optional[float] = None
    
    # Métadonnées
    fetched_at: datetime = Field(default_factory=datetime.now)
    quality_score: Optional[float] = None
    
    @validator('latitude', 'longitude')
    def check_coordinates(cls, v):
        if v is None:
            raise ValueError('Latitude and Longitude must be present')
        return v


# --- Modèle GeocodingResult (Pour l'enrichissement) ---
class GeocodingResult(BaseModel):
    """Résultat de géocodage."""
    original_address: str
    label: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    score: float = 0.0
    postal_code: Optional[str] = None
    city_code: Optional[str] = None
    city: Optional[str] = None
    
    @property
    def is_valid(self) -> bool:
        return self.score >= 0.5 and self.latitude is not None


# --- Modèle QualityMetrics (Reste inchangé) ---
class QualityMetrics(BaseModel):
    """Métriques de qualité du dataset."""
    total_records: int
    valid_records: int
    completeness_score: float
    duplicates_count: int
    duplicates_pct: float
    geocoding_success_rate: float
    avg_geocoding_score: float
    null_counts: dict
    quality_grade: str  # A, B, C, D, F
    
    @property
    def is_acceptable(self) -> bool:
        return self.quality_grade in ['A', 'B', 'C']