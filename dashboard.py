import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from pipeline.quality import QualityAnalyzer
from pipeline.config import PROCESSED_DIR

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Qualité Météo",
    page_icon="🌤️",
    layout="wide"
)

def load_latest_data():
    """Charge le fichier Parquet le plus récent."""
    if not PROCESSED_DIR.exists():
        return None, None
    
    files = sorted(PROCESSED_DIR.glob("*.parquet"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not files:
        return None, None
        
    latest_file = files[0]
    df = pd.read_parquet(latest_file)
    
    # S'assurer que la colonne date est bien au format datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        
    return df, latest_file.name

# --- Titre et En-tête ---
st.title("🌤️ Dashboard de Qualité des Données Météo")
st.markdown("Visualisation interactive du pipeline ETL et audit de qualité automatique.")

# --- Chargement des données ---
with st.spinner("Chargement des données..."):
    df, filename = load_latest_data()

if df is None:
    st.error("❌ Aucune donnée trouvée. Veuillez lancer le pipeline d'abord : `uv run python -m pipeline.main`")
    st.stop()

st.success(f"📂 Données chargées depuis : **{filename}**")

# --- Sidebar (Filtres & Options) ---
with st.sidebar:
    st.header("⚙️ Options")
    
    # 1. Option de Carte
    st.subheader("🗺️ Carte")
    map_option = st.radio(
        "Afficher la météo pour :",
        ["Aujourd'hui", "Demain"],
        index=0
    )
    
    st.markdown("---")
    
    # 2. Filtre Villes
    st.subheader("🏙️ Filtre Villes")
    selected_cities = []
    
    if 'original_city_name' in df.columns:
        # Liste de toutes les villes disponibles
        all_cities = sorted(df['original_city_name'].unique())
        
        # Définir la valeur par défaut (Avignon si disponible, sinon vide)
        default_selection = ["Avignon"] if "Avignon" in all_cities else []
        
        selected_cities = st.multiselect(
            "Sélectionner des villes", 
            options=all_cities,
            default=default_selection  # <--- Pré-sélection ici
        )
        
        if selected_cities:
            st.info(f"Filtre actif : {len(selected_cities)} villes")
    
    st.markdown("---")
    st.caption("Dashboard v1.3 - Open Data")

# --- Filtrage Global (pour l'affichage table et graphiques) ---
df_display = df[df['original_city_name'].isin(selected_cities)] if selected_cities else df

# --- KPI (Indicateurs Clés) ---
analyzer = QualityAnalyzer(df)
metrics = analyzer.analyze()

st.subheader("📊 Métriques de Qualité (Global)")
col1, col2, col3, col4 = st.columns(4)
with col1: st.metric("Note Globale", metrics.quality_grade, border=True)
with col2: st.metric("Complétude", f"{metrics.completeness_score * 100:.1f}%", border=True)
with col3: st.metric("Doublons", f"{metrics.duplicates_pct:.1f}%", delta_color="inverse", border=True)
with col4: st.metric("Enregistrements", len(df), border=True)

# --- CARTE INTERACTIVE ---
st.markdown("---")
st.subheader(f"🗺️ Carte des Températures ({map_option})")

# Logique pour trouver la date cible
sorted_dates = sorted(df['date'].unique())
target_date = sorted_dates[0] if map_option == "Aujourd'hui" else sorted_dates[1]
target_date_str = target_date.strftime('%d/%m/%Y')

# Filtrer les données pour la carte (Date précise + Vue d'ensemble géographique)
df_map = df[df['date'] == target_date].copy()

if not df_map.empty:
    fig_map = px.scatter_mapbox(
        df_map,
        lat="latitude",
        lon="longitude",
        color="temperature_max",
        size="temperature_max",
        size_max=15,
        hover_name="original_city_name",
        hover_data={"temperature_max": True, "temperature_min": True, "latitude": False, "longitude": False},
        color_continuous_scale="RdYlBu_r",
        zoom=4.5,
        center={"lat": 46.603354, "lon": 1.888334},
        title=f"Températures Max le {target_date_str}",
        mapbox_style="open-street-map"
    )
    fig_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.warning(f"Pas de données disponibles pour {map_option}.")

# --- Graphiques Détaillés par Ville ---
if selected_cities:
    st.markdown("---")
    st.subheader("📈 Prévisions par Ville Sélectionnée")
    
    for city in selected_cities:
        city_data = df[df['original_city_name'] == city].sort_values('date')
        
        fig = px.line(
            city_data, 
            x='date', 
            y=['temperature_max', 'temperature_min'],
            title=f"Prévisions pour {city}",
            labels={'value': 'Température (°C)', 'date': 'Date', 'variable': 'Type'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

# --- Explorateur de Données ---
st.markdown("---")
st.subheader("🔍 Explorateur de Données")
st.caption(f"Aperçu des 25 premières lignes pour la sélection actuelle.")
st.dataframe(df_display.head(25), use_container_width=True)