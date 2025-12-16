"""Tests pour le transformer (Adapté Météo)."""
import pytest
import pandas as pd
import numpy as np
from pipeline.transformer import DataTransformer


class TestDataTransformer:
    """Tests pour DataTransformer."""
    
    @pytest.fixture
    def sample_df(self):
        """DataFrame de test Météo/Géo."""
        return pd.DataFrame({
            'date': ['2025-12-01', '2025-12-02', '2025-12-01', '2025-12-03'],
            'latitude': [48.8, 48.8, 48.8, 45.7],
            'longitude': [2.3, 2.3, 2.3, 4.8],
            'validated_city': ['  Paris  ', None, 'Paris', 'Lyon'],
            'temperature_max': [10.0, None, 10.0, 100.0], # 100 est un outlier
            'precipitation_sum': [0.0, 5.0, 0.0, 50.0]
        })
    
    def test_remove_duplicates(self, sample_df):
        """Test la suppression des doublons sur l'ID composé."""
        transformer = DataTransformer(sample_df)
        # ID composé: date, lat, lon
        result = transformer.remove_duplicates(subset=['date', 'latitude', 'longitude']).get_result()
        
        assert len(result) == 3 # L'enregistrement du 2025-12-01 (index 2) est supprimé
        
    def test_handle_missing_values_median(self, sample_df):
        """Test le remplacement par la médiane pour temperature_max."""
        transformer = DataTransformer(sample_df)
        # La médiane de [10.0, 10.0, 100.0] est 10.0
        result = transformer.handle_missing_values(numeric_strategy='median').get_result()
        
        assert result['temperature_max'].isnull().sum() == 0
        # Le None (index 1) est remplacé par la médiane 10.0
        assert result.loc[1, 'temperature_max'] == 10.0 
    
    def test_normalize_text(self, sample_df):
        """Test la normalisation du texte sur le nom de ville."""
        transformer = DataTransformer(sample_df)
        result = transformer.normalize_text_columns(['validated_city']).get_result()
        
        assert 'paris' in result['validated_city'].values
        assert result.loc[0, 'validated_city'] == 'paris'
    
    def test_add_derived_columns(self, sample_df):
        """Test l'ajout des colonnes dérivées spécifiques à la météo."""
        # On ajoute une colonne temporaire pour tester l'amplitude
        df_test = sample_df.copy()
        df_test['temperature_min'] = [5.0, 0.0, 5.0, 50.0]
        
        transformer = DataTransformer(df_test)
        result = transformer.add_derived_columns().get_result()
        
        # Test de l'amplitude (10 - 5 = 5.0)
        assert 'temperature_amplitude' in result.columns
        assert result.loc[0, 'temperature_amplitude'] == 5.0
        
        # Test de la catégorie de précipitation (50.0 doit être 'forte')
        assert 'precipitation_category' in result.columns
        assert result.loc[3, 'precipitation_category'] == 'forte'
        
    def test_chaining(self, sample_df):
        """Test le chaînage des transformations."""
        transformer = DataTransformer(sample_df)
        result = (
            transformer
            .remove_duplicates(['date', 'latitude', 'longitude'])
            .handle_missing_values()
            .normalize_text_columns(['validated_city'])
            .get_result()
        )
        
        assert len(transformer.transformations_applied) >= 3