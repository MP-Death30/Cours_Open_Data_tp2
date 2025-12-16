"""Module de scoring et rapport de qualité."""
import pandas as pd
from datetime import datetime
from pathlib import Path
from litellm import completion, AuthenticationError
from dotenv import load_dotenv
import os
import logging

from .config import QUALITY_THRESHOLDS, REPORTS_DIR
from .models import QualityMetrics

load_dotenv()
logger = logging.getLogger(__name__)


class QualityAnalyzer:
    """Analyse et score la qualité des données."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.metrics = None
    
    def calculate_completeness(self) -> float:
        """Calcule le score de complétude (% de valeurs non-nulles)."""
        total_cells = self.df.size
        non_null_cells = self.df.notna().sum().sum()
        return non_null_cells / total_cells if total_cells > 0 else 0
    
    def count_duplicates(self) -> tuple[int, float]:
        """Compte les doublons en utilisant date et nom de ville."""
        
        # L'ID qui aurait dû être unique si les coordonnées n'étaient pas forcées
        # Mais nous utilisons les champs textuels pour ignorer la faute de simulation
        id_col = ['date', 'original_city_name'] 
        
        duplicates = self.df.duplicated(subset=id_col).sum()
        pct = duplicates / len(self.df) * 100 if len(self.df) > 0 else 0
        
        return duplicates, pct
    
    def calculate_geocoding_stats(self) -> tuple[float, float]:
        """Calcule les stats de géocodage si applicable."""
        if 'geocoding_score' not in self.df.columns:
            return 0, 0
        
        valid_geo = self.df['geocoding_score'].notna() & (self.df['geocoding_score'] > 0)
        success_rate = valid_geo.sum() / len(self.df) * 100 if len(self.df) > 0 else 0
        avg_score = self.df.loc[valid_geo, 'geocoding_score'].mean() if valid_geo.any() else 0
        
        return success_rate, avg_score
    
    def calculate_null_counts(self) -> dict:
        """Compte les valeurs nulles par colonne."""
        return self.df.isnull().sum().to_dict()
    
    def determine_grade(self, completeness: float, duplicates_pct: float, geo_rate: float) -> str:
        """Détermine la note de qualité globale."""
        score = 0
        
        # Complétude (40 points max)
        score += min(completeness * 40, 40)
        
        # Doublons (30 points max)
        if duplicates_pct <= 1:
            score += 30
        elif duplicates_pct <= 5:
            score += 20
        elif duplicates_pct <= 10:
            score += 10
        
        # Géocodage (30 points max) - si applicable
        if 'geocoding_score' in self.df.columns:
            score += min(geo_rate / 100 * 30, 30)
        else:
            score += 30  # Pas de pénalité si pas de géocodage
        
        # Note finale
        if score >= 90:
            return 'A'
        elif score >= 75:
            return 'B'
        elif score >= 60:
            return 'C'
        elif score >= 40:
            return 'D'
        else:
            return 'F'
    
    def analyze(self) -> QualityMetrics:
        """Effectue l'analyse complète de qualité."""
        completeness = self.calculate_completeness()
        duplicates, duplicates_pct = self.count_duplicates()
        geo_rate, geo_avg = self.calculate_geocoding_stats()
        null_counts = self.calculate_null_counts()
        
        valid_records = len(self.df) - duplicates
        
        grade = self.determine_grade(completeness, duplicates_pct, geo_rate)
        
        self.metrics = QualityMetrics(
            total_records=len(self.df),
            valid_records=valid_records,
            completeness_score=round(completeness, 3),
            duplicates_count=duplicates,
            duplicates_pct=round(duplicates_pct, 2),
            geocoding_success_rate=round(geo_rate, 2),
            avg_geocoding_score=round(geo_avg, 3),
            null_counts=null_counts,
            quality_grade=grade,
        )
        
        return self.metrics
    
    def generate_ai_recommendations(self) -> str:
        """Génère des recommandations via l'IA avec fallback manuel."""
        if not self.metrics:
            self.analyze()
        
        context = f"""
        Analyse de qualité d'un dataset Météo/Géo :
        - Total: {self.metrics.total_records} enregistrements
        - Complétude: {self.metrics.completeness_score * 100:.1f}%
        - Doublons: {self.metrics.duplicates_pct:.1f}%
        - Note: {self.metrics.quality_grade}
        
        Valeurs nulles par colonne:
        {self.metrics.null_counts}
        """
        
        # 1. DÉFINITION DE LA STRATÉGIE DE FALLBACK MANUELLE
        # Définissez les modèles dans l'ordre de priorité
        models_to_try = [
            {"model": "gemini/gemini-2.5-flash-lite"}, # 1. Tentative Gemini (échouera sans clé)
            {"model": "ollama/mistral", "api_base": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")} # 2. Tentative Ollama local
        ]
        
        messages = [
            {"role": "system", "content": "Tu es un expert en qualité des données. Donne des recommandations concrètes et actionnables."},
            {"role": "user", "content": f"{context}\n\nQuelles sont tes 5 recommandations prioritaires pour améliorer ce dataset ?"}
        ]
        
        # 2. BOUCLE DE FALLBACK MANUELLE
        for config in models_to_try:
            model_name = config['model']
            api_base = config.get('api_base')
            
            logger.info(f"Tentative avec le modèle : {model_name}")
            print(f"🤖 Tentative IA : {model_name}...")
            
            try:
                # Appelle la fonction completion avec la configuration du modèle
                response = completion(
                    model=model_name,
                    messages=messages,
                    api_base=api_base # Sera None pour Gemini, la valeur locale pour Ollama
                )
                
                # Succès : Retourne la recommandation
                return response.choices[0].message.content
            
            except AuthenticationError:
                # Si Gemini échoue à cause de la clé, c'est normal, on passe au fallback.
                logger.warning(f"Clé API invalide pour {model_name}. Tentative de fallback.")
                continue
            except Exception as e:
                # Gère toutes les autres erreurs (connexion, modèle absent, etc.)
                logger.error(f"Erreur lors de l'appel à {model_name}: {e}")
                continue # Passe au modèle suivant
        
        # 3. Échec total
        return "❌ Recommandations IA indisponibles. Erreur d'authentification ou Ollama n'est pas lancé/configuré."
    
    def generate_report(self, output_name: str = "quality_report") -> Path:
        """Génère un rapport de qualité complet en Markdown."""
        if not self.metrics:
            self.analyze()
        
        recommendations = self.generate_ai_recommendations()
        
        report = f"""# Rapport de Qualité des Données

**Généré le** : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Métriques Globales

| Métrique | Valeur | Seuil |
|----------|--------|-------|
| **Note globale** | **{self.metrics.quality_grade}** | A-B-C = Acceptable |
| Total enregistrements | {self.metrics.total_records} | - |
| Enregistrements valides | {self.metrics.valid_records} | - |
| Complétude | {self.metrics.completeness_score * 100:.1f}% | ≥ 70% |
| Doublons | {self.metrics.duplicates_pct:.1f}% | ≤ 5% |
| Géocodage réussi | {self.metrics.geocoding_success_rate:.1f}% | ≥ 50% |
| Score géocodage moyen | {self.metrics.avg_geocoding_score:.2f} | ≥ 0.5 |

## 📋 Valeurs Manquantes par Colonne

| Colonne | Valeurs nulles | % |
|---------|----------------|---|
"""
        
        for col, count in sorted(self.metrics.null_counts.items(), key=lambda x: x[1], reverse=True):
            pct = count / self.metrics.total_records * 100 if self.metrics.total_records > 0 else 0
            report += f"| {col} | {count} | {pct:.1f}% |\n"
        
        report += f"""

## 🤖 Recommandations IA

{recommendations}

## ✅ Conclusion

{"✅ **Dataset acceptable** pour l'analyse." if self.metrics.is_acceptable else "⚠️ **Dataset nécessite des corrections** avant utilisation."}

---
*Rapport généré automatiquement par le pipeline Open Data*
"""
        
        # Sauvegarder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = REPORTS_DIR / f"{output_name}_{timestamp}.md"
        filepath.write_text(report, encoding='utf-8')
        
        print(f"📄 Rapport sauvegardé : {filepath}")
        return filepath