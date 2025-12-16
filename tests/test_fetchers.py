"""Tests pour les fetchers."""
import pytest
from pipeline.fetchers.openmeteo import OpenMeteoFetcher 
from pipeline.fetchers.adresse import AdresseFetcher


class TestOpenMeteoFetcher:
    """Tests pour OpenMeteoFetcher."""
    
    # CECI EST UN TEST QUI FAIT UN APPEL API REEL
    def test_fetch_weather_returns_dict(self):
        """Test que fetch_weather_by_coords retourne un dictionnaire."""
        fetcher = OpenMeteoFetcher()
        # Coordonnées de Paris
        result = fetcher.fetch_weather_by_coords(48.85, 2.35) 
        
        assert isinstance(result, dict)
        assert "daily" in result
        
    # CECI EST UN TEST QUI UTILISE LA PAGINATION (fetch_all) ET L'IMPLÉMENTATION DE LA CLASSE
    def test_fetch_all_returns_generator(self):
        """Test que fetch_all retourne bien des enregistrements."""
        fetcher = OpenMeteoFetcher()
        # Tester avec un petit sous-ensemble de villes (pour ne pas être trop long)
        results = list(fetcher.fetch_all(["Paris", "Lyon"], max_items=2, verbose=False))
        
        # Chaque ville doit avoir 7 jours de prévisions
        assert len(results) >= 14 # 2 villes x 7 jours
        assert "original_city_name" in results[0]


class TestAdresseFetcher:
    """Tests pour AdresseFetcher."""
    
    def test_geocode_single_valid_address(self):
        """Test le géocodage d'une adresse valide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("20 avenue de ségur paris")
        
        assert result.original_address == "20 avenue de ségur paris"
        assert result.score > 0.5
        assert result.latitude is not None
        assert result.longitude is not None
    
    def test_geocode_single_invalid_address(self):
        """Test le géocodage d'une adresse invalide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("xyzabc123456")
        
        assert result.score < 0.5 or result.latitude is None
    
    def test_geocode_empty_address(self):
        """Test le géocodage d'une adresse vide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("")
        
        assert result.score == 0