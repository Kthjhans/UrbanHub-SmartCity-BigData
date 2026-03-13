import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

def plot_years():
    print("Chargement des données pour le graphique temporel...")
    files = glob.glob('Data/Clean/weather_*.parquet')
    if not files:
        print("Aucun fichier nettoyé trouvé dans Data/Clean. Veuillez lancer cleaning.py d'abord.")
        return

    all_dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(all_dfs)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Agrégation par mois pour voir l'évolution sur 15 ans
    print("Calcul de l'évolution mensuelle...")
    df['year_month'] = df['timestamp'].dt.to_period('M')
    trend = df.groupby('year_month')['temperature'].mean().reset_index()
    trend['year_month'] = trend['year_month'].dt.to_timestamp()

    plt.figure(figsize=(15, 7))
    plt.plot(trend['year_month'], trend['temperature'], color='red', linewidth=1, label='Température Moyenne Mensuelle')
    plt.title("Évolution de la température moyenne en France (2009-2025)")
    plt.xlabel("Années")
    plt.ylabel("Température Moyenne (°C)")
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend()
    
    plt.savefig("weather_evolution_years.png")
    print("\nSuccès ! Le graphique 'weather_evolution_years.png' a été créé.")
    print("Il montre la courbe complète des 15 dernières années.")

if __name__ == "__main__":
    plot_years()
