import requests
import pandas as pd
import time
import os
from datetime import datetime

# Liste des réseaux cibles (Principales villes de France)
NETWORKS = {
    'velib': 'Paris',
    'velov': 'Lyon',
    'le-velo': 'Marseille',
    'velo': 'Toulouse',
    'v3-bordeaux': 'Bordeaux',
    'bicloo': 'Nantes'
}

OUTPUT_DIR = 'Data/Streaming'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'mobility_data.csv')

# Création du dossier de stockage si nécessaire
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def get_bike_data():
    all_data = []
    print(f"\n--- Collecte du {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    for net_id, city_name in NETWORKS.items():
        try:
            url = f"https://api.citybik.es/v2/networks/{net_id}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            stations = data['network']['stations']
            
            for s in stations:
                all_data.append({
                    'city': city_name,
                    'station_id': s['id'],
                    'station_name': s['name'],
                    'latitude': s['latitude'],
                    'longitude': s['longitude'],
                    'free_slots': s.get('empty_slots', 0),
                    'bikes_available': s.get('free_bikes', 0),
                    'timestamp': s['timestamp']
                })
            print(f"  [OK] {city_name} : {len(stations)} stations.")
        except Exception as e:
            print(f"  [ERREUR] {city_name} : {e}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        # Ajout au fichier CSV (mode 'a' pour append)
        header = not os.path.exists(OUTPUT_FILE)
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=header)
        print(f"Données enregistrées dans {OUTPUT_FILE}")

def main():
    print("="*50)
    print("URBANHUB - COLLECTE MOBILITÉ (CityBikes)")
    print("Fréquence : Toutes les 60 secondes")
    print("Appuyez sur Ctrl+C pour arrêter le script.")
    print("="*50)
    
    try:
        while True:
            get_bike_data()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nCollecte arrêtée par l'utilisateur.")

if __name__ == "__main__":
    main()
