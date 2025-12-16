"""Module de stockage des données."""
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

from .config import RAW_DIR, PROCESSED_DIR

logger = logging.getLogger(__name__)

def save_raw_json(data: list[dict], name: str) -> Path:
    """Sauvegarde les données brutes en JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DIR / f"{name}_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Brut: {filepath.name} ({size_kb:.1f} KB)")
    
    return filepath


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """Sauvegarde le DataFrame en Parquet."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROCESSED_DIR / f"{name}_{timestamp}.parquet"
    
    df.to_parquet(filepath, index=False, compression="snappy")
    
    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Parquet: {filepath.name} ({size_kb:.1f} KB)")
    
    return filepath


def load_parquet(filepath: str | Path) -> pd.DataFrame:
    """Charge un fichier Parquet."""
    return pd.read_parquet(filepath)


class StorageManager:
    """Gère les opérations de stockage et la vérification incrémentale (Bonus)."""
    
    def file_exists_for_today(self, directory: Path, prefix: str) -> bool:
        """Vérifie si des fichiers avec un préfixe donné existent pour la date du jour."""
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Cherche le rapport de qualité, car c'est le dernier livrable
        # (Nous vérifions le rapport car il est créé à la fin du pipeline)
        matches = list(directory.glob(f"{prefix}_{date_str}_*.md")) 
        
        return len(matches) > 0