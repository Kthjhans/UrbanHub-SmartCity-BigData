import os
import pandas as pd
import numpy as np
from datetime import datetime
import glob

# Configuration
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_PATH, "Data", "Raw")
CLEAN_DIR = os.path.join(BASE_PATH, "Data", "Clean")

def parse_tmp(val):
    """Parse TMP column: +0020,1 -> 2.0"""
    try:
        parts = val.split(',')
        temp = float(parts[0])
        quality = parts[1]
        if temp == 9999 or quality in ['2', '3', '6', '7']: # Qualité suspecte
            return np.nan
        return temp / 10.0
    except:
        return np.nan

def parse_wnd(val):
    """Parse WND column: 110,1,N,0026,1 -> (110.0, 2.6)"""
    try:
        parts = val.split(',')
        direction = float(parts[0])
        d_quality = parts[1]
        speed = float(parts[3])
        s_quality = parts[4]
        
        res_dir = direction if direction != 999 and d_quality not in ['2','3','6','7'] else np.nan
        res_speed = speed / 10.0 if speed != 9999 and s_quality not in ['2','3','6','7'] else np.nan
        return res_dir, res_speed
    except:
        return np.nan, np.nan

def parse_vis(val):
    """Parse VIS: 009900,5,9,9 -> 9900.0"""
    try:
        parts = val.split(',')
        dist = float(parts[0])
        quality = parts[1]
        if dist == 999999 or quality in ['2','3','6','7']:
            return np.nan
        return dist
    except:
        return np.nan

def parse_aa1(val):
    """Parse AA1 column: 01,0000,9,1 -> 0.0 (mm)"""
    try:
        if pd.isna(val) or val == '': return 0.0
        parts = val.split(',')
        depth = float(parts[1])
        quality = parts[3]
        if depth == 9999 or quality in ['2','3','6','7']:
            return np.nan
        return depth / 10.0 # Convertir en mm
    except:
        return 0.0

def clean_year(year):
    print(f"Nettoyage de l'année {year}...")
    files = glob.glob(os.path.join(RAW_DIR, str(year), "*.csv"))
    if not files:
        return None
    
    all_data = []
    for f in files:
        try:
            # Vérifier quelles colonnes existent réellement
            cols_to_use = ['STATION', 'DATE', 'TMP', 'WND', 'VIS', 'SLP']
            temp_df = pd.read_csv(f, nrows=0)
            if 'AA1' in temp_df.columns:
                cols_to_use.append('AA1')
            
            df = pd.read_csv(f, low_memory=False, usecols=cols_to_use)
            
            # Extraction des variables
            df['temperature'] = df['TMP'].apply(parse_tmp)
            df['wind_direction'], df['wind_speed'] = zip(*df['WND'].apply(parse_wnd))
            df['visibility'] = df['VIS'].apply(parse_vis)
            df['pressure'] = df['SLP'].apply(parse_slp)
            df['precipitation'] = df['AA1'].apply(parse_aa1) if 'AA1' in df.columns else 0.0
            
            df['timestamp'] = pd.to_datetime(df['DATE'])
            df['station_id'] = df['STATION']
            
            # Sélection des colonnes finales
            cols_final = ['station_id', 'timestamp', 'temperature', 'wind_direction', 'wind_speed', 'visibility', 'pressure', 'precipitation']
            df = df[[c for c in cols_final if c in df.columns]]
            
            # Suppression des lignes totalement vides (hors ID/TS)
            df = df.dropna(subset=['temperature', 'wind_speed', 'visibility', 'pressure'], how='all')
            
            all_data.append(df)
        except Exception as e:
            print(f"Erreur sur {f}: {e}")
            
    if all_data:
        annual_df = pd.concat(all_data, ignore_index=True)
        os.makedirs(CLEAN_DIR, exist_ok=True)
        target = os.path.join(CLEAN_DIR, f"weather_{year}.parquet")
        annual_df.to_parquet(target, index=False)
        print(f"Sauvegardé : {target} ({len(annual_df)} lignes)")
        return annual_df
    return None

def main():
    years = [d for d in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, d))]
    years.sort(reverse=True)
    
    for y in years:
        clean_year(y)

if __name__ == "__main__":
    main()
