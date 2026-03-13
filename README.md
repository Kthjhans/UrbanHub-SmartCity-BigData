# UrbanHub - Jumeau Numérique Urbain (Smart City Big Data)

##  Contexte du Projet
Une **Smart City** génère en permanence des données massives provenant de multiples sources (IoT, capteurs environnementaux, flux de trafic, mobilité douce). Ces données doivent être collectées, stockées et analysées afin de produire des indicateurs utiles aux décideurs publics.

**UrbanHub** est une plateforme capable d'ingérer et d'analyser ces flux typiques d'un système Big Data réel :
1. **Flux Batch** (Données historiques massives)
2. **Flux Streaming** (Données temps réel)
3. **Flux IoT** (Données capteurs)

##  Résultats Visés
- **Observabilité** : Visualiser l'état de la ville en temps réel (ex: disponibilité des vélos).
- **Analyse Historique** : Identifier des tendances climatiques et des anomalies météorologiques depuis 1990.
- **Aide à la décision** : Localiser les zones de tension (stations de vélos saturées ou vides) pour optimiser les services urbains.

---

##  Architecture Technique
Les données suivent un pipeline classique de Data Engineering :
`Ingestion → Stockage (Data Lake) → Nettoyage → Analyse → Visualisation`

---

##  Fonctionnement et Lancement

### Partie 1 : Flux Batch (Analyse Météorologique)
Cette partie traite les données mondiales horaires de la NOAA pour les stations françaises depuis 1990.

1. **Ingestion Parallèle** :
   ```bash
   python ingestion.py
   ```
   *Télécharge les fichiers CSV des stations FR de manière optimisée.*

2. **Nettoyage et Transformation** :
   ```bash
   python cleaning.py
   ```
   *Extrait les variables (Température, Vent, Pluie, Pression) et convertit les données au format Parquet (plus performant).*

3. **Analyse Métier** :
   ```bash
   python analysis.py
   ```
   *Génère les statistiques sur les anomalies, les corrélations et les records météorologiques.*

4. **Visualisation Interactive** :
   *Ouvrir `weather_analysis.ipynb` dans VS Code pour voir les graphiques de tendances.*

### Partie 2 : Flux Streaming (Mobilité CityBikes)
Analyse en temps réel de la disponibilité des vélos en libre-service.

1. **Analyse et Cartographie** :
   *Ouvrir `mobility_analysis.ipynb`.*
   - **Nettoyage** : Correction automatique des formats de date ISO8601.
   - **Calculs** : Taux d'occupation par station.
   - **Visualisation** : Génération d'une carte `folium` interactive localisant toutes les stations vides en France.

---

##  Installation
```bash
# Installation des dépendances nécessaires
pip install pandas numpy matplotlib seaborn requests folium pyarrow tqdm
```

##  Structure du Dépôt
- `ingestion.py` : Script de collecte Batch.
- `cleaning.py` : Pipeline de nettoyage des données brutes.
- `analysis.py` : Calcul des indicateurs clés.
- `*.ipynb` : Notebooks d'analyse et de visualisation.
- `.gitignore` : Protection contre l'envoi de données volumineuses (Data/).
