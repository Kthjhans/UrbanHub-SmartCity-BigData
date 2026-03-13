import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Configuration
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
CLEAN_DIR = os.path.join(BASE_PATH, "Data", "Clean")
HISTORY_FILE = os.path.join(BASE_PATH, "isd-history.csv")

def load_data():
    """Charge tous les fichiers Parquet nettoyés."""
    import glob
    files = glob.glob(os.path.join(CLEAN_DIR, "*.parquet"))
    if not files:
        print("Aucune donnée nettoyée trouvée. Lancez cleaning.py d'abord.")
        return None
    
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

def get_station_mapping():
    """Crée un mapping station_id -> City Name."""
    df_h = pd.read_csv(HISTORY_FILE, low_memory=False)
    def format_id(row):
        try:
            return f"{str(int(row['USAF'])).zfill(6)}{str(int(row['WBAN'])).zfill(5)}"
        except: return None
    
    df_h['station_id'] = df_h.apply(format_id, axis=1)
    return df_h.set_index('station_id')['STATION NAME'].to_dict()

def perform_analysis(df, mapping):
    print("\n" + "="*50)
    print("      RÉSULTATS DE L'ANALYSE MÉTIER (PARTIE 1)")
    print("="*50)
    
    df['city'] = df['station_id'].map(mapping)
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    df['day'] = df['timestamp'].dt.date
    
    # Q1: Périodes météorologiques anormales
    # Définition : Observations où la température s'écarte de plus de 3 écart-types de la moyenne mensuelle
    monthly_stats = df.groupby('month')['temperature'].agg(['mean', 'std']).reset_index()
    df_anom = df.merge(monthly_stats, on='month', suffixes=('', '_month'))
    df_anom['z_score'] = (df_anom['temperature'] - df_anom['mean_month']) / df_anom['std_month']
    anomalies = df_anom[np.abs(df_anom['z_score']) > 3]
    
    print(f"\n1. IDENTIFICATION DES ANOMALIES :")
    print(f"   - Nombre total d'observations considérées comme anormales : {len(anomalies)}")
    if not anomalies.empty:
        print("   - Exemple d'anomalie marquée :")
        print(anomalies.sort_values('z_score', ascending=False).head(3)[['timestamp', 'city', 'temperature', 'z_score']])

    # Q2: Corrélation conditions météo / visibilité
    cols_corr = ['temperature', 'wind_speed', 'visibility', 'pressure', 'precipitation']
    corr = df[[c for c in cols_corr if c in df.columns]].corr()
    print("\n2. CORRÉLATION MÉTÉO VS VISIBILITÉ :")
    print(corr['visibility'].sort_values(ascending=False))
    print("   Note : Une corrélation négative avec l'humidité ou les précipitations est attendue.")

    # Q3: Évolution saisonnière de la température
    seasonal = df.groupby('month')['temperature'].agg(['mean', 'min', 'max'])
    print("\n3. ÉVOLUTION SAISONNIÈRE (Moyenne nationale) :")
    print(seasonal)

    # Q4: Jours avec conditions extrêmes
    print("\n4. JOURS ET STATIONS AVEC CONDITIONS EXTRÊMES :")
    # Vent extrême
    max_wind = df.loc[df['wind_speed'].idxmax()]
    print(f"   - Vent record : {max_wind['wind_speed']} m/s à {max_wind['city']} le {max_wind['timestamp']}")
    # Chaleur extrême
    max_temp = df.loc[df['temperature'].idxmax()]
    print(f"   - Chaleur record : {max_temp['temperature']} °C à {max_temp['city']} le {max_temp['timestamp']}")
    # Précipitations extrêmes
    if 'precipitation' in df.columns and df['precipitation'].max() > 0:
        max_precip = df.loc[df['precipitation'].idxmax()]
        print(f"   - Pluie record : {max_precip['precipitation']} mm à {max_precip['city']} le {max_precip['timestamp']}")

    # Visualisation (Optimisée pour gros volumes)
    print("\nGénération du graphique (agrégation des données)...")
    plt.figure(figsize=(12, 6))
    
    # On agrège par mois pour le tracé (beaucoup plus rapide)
    df_plot = df.groupby('month')['temperature'].agg(['mean', 'min', 'max']).reset_index()
    
    plt.plot(df_plot['month'], df_plot['mean'], label='Moyenne', color='blue', marker='o')
    plt.fill_between(df_plot['month'], df_plot['min'], df_plot['max'], alpha=0.2, label='Amplitude (Min-Max)', color='blue')
    
    plt.xticks(range(1, 13), ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sept', 'Oct', 'Nov', 'Déc'])
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.title("Évolution saisonnière de la température en France (Données Historiques)")
    plt.xlabel("Mois")
    plt.ylabel("Température (°C)")
    
    plt.savefig("seasonal_temp.png")
    print("\nGraphique 'seasonal_temp.png' généré avec succès.")

def main():
    df = load_data()
    if df is not None:
        mapping = get_station_mapping()
        perform_analysis(df, mapping)

if __name__ == "__main__":
    main()
