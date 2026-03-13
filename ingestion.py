import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import urllib3
from tqdm import tqdm

# Désactiver les avertissements SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.ncei.noaa.gov/data/global-hourly/access/"
HISTORY_URLS = [
    "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv",
    "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
]

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_PATH, "Data", "Raw")
HISTORY_FILE = os.path.join(BASE_PATH, "isd-history.csv")

START_YEAR = 1990
CURRENT_YEAR = datetime.now().year
MAX_WORKERS = 10 

def download_file(url, target_path, skip_if_exists=True):
    """Télécharge un fichier avec gestion d'erreur."""
    if skip_if_exists and os.path.exists(target_path):
        return "skipped"
    
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    
    for verify in [True, False]:
        try:
            response = requests.get(url, timeout=15, verify=verify)
            if response.status_code == 200:
                with open(target_path, 'wb') as f:
                    f.write(response.content)
                return "downloaded"
            elif response.status_code == 404:
                return "404"
        except:
            pass
    return "error"

def main():
    print("\n--- UrbanHub Ingestion Parallel ---")
    
    if not os.path.exists(HISTORY_FILE):
        print("Téléchargement de l'historique...")
        for url in HISTORY_URLS:
            if download_file(url, HISTORY_FILE, skip_if_exists=False) == "downloaded":
                break

    print("Identification des stations FR...")
    df = pd.read_csv(HISTORY_FILE, low_memory=False)
    fr_stations = df[(df['CTRY'] == 'FR') & (df['END'] >= START_YEAR * 10000)].copy()
    
    def format_id(row):
        try:
            return f"{str(int(row['USAF'])).zfill(6)}{str(int(row['WBAN'])).zfill(5)}"
        except: return None

    fr_stations['station_id'] = fr_stations.apply(format_id, axis=1)
    fr_stations = fr_stations.dropna(subset=['station_id'])
    station_ids = fr_stations['station_id'].unique()
    
    # Traitement par année pour répondre à la demande de l'utilisateur
    years = range(CURRENT_YEAR, START_YEAR - 1, -1)
    
    print(f"Démarrage du téléchargement pour {len(station_ids)} stations sur {len(years)} ans.")
    
    total_new = 0
    for year in years:
        tasks = []
        for s_id in station_ids:
            url = f"{BASE_URL}{year}/{s_id}.csv"
            target = os.path.join(DATA_DIR, str(year), f"{s_id}.csv")
            tasks.append((url, target))
        
        # Barre de progression pour l'année en cours
        pbar = tqdm(total=len(tasks), desc=f"Année {year}", unit="station")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(download_file, url, target): url for url, target in tasks}
            
            for future in as_completed(future_to_url):
                res = future.result()
                if res == "downloaded":
                    total_new += 1
                pbar.update(1)
        
        pbar.close()

    print(f"\nIngestion terminée : {total_new} nouveaux fichiers téléchargés.")

if __name__ == "__main__":
    main()
