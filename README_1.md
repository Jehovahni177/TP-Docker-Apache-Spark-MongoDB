# Transformation CSV vers JSON avec Apache Spark

Un pipeline de traitement de données robuste et scalable pour convertir des données de logements régionaux (format CSV) vers le format JSON, avec nettoyage automatique des données.

## Description du projet

Ce projet démontre l'utilisation de **Apache Spark** pour traiter des données volumineuses et effectuer des transformations de format.

### Fonctionnalités principales

- **Lecture de fichiers CSV** : Import de données tabulaires avec séparateur personnalisé (`;`)
- **Nettoyage des données** : 
  - Normalisation des noms de colonnes (minuscules, remplacement des espaces par `_`)
  - Suppression des caractères spéciaux
  - Suppression automatique des doublons
- **Inférence de schéma** : Détection automatique des types de données
- **Export JSON** : Génération de fichiers JSON partitionnés et optimisés
- **Logging détaillé** : Affichage du schéma et des statistiques de traitement

## 🗂️ Structure du projet

```
tp_docker_spark_mongo/
├── README.md                      # Documentation du projet
├── scripts/
│   └── csv_to_json.py            # Script principal de transformation
├── data/
│   └── logements_regions.csv      # Données d'entrée (régions françaises)
└── output/
    └── logements_regions_json/    # Données de sortie (format JSON)
        ├── _SUCCESS               # Fichier marqueur de succès
        └── part-00000-*.json      # Fichiers JSON partitionnés
```

## 📊 Source de données

### Format CSV
Les données sources contiennent des informations sur les logements en France par région :

| Colonne | Description |
|---------|-------------|
| `année_publication` | Année de publication des données |
| `code_région` | Code numéraire unique de la région |
| `nom_region` | Nom de la région française |
| `Nombre d'habitants` | Population totale |
| `Densité de population au km²` | Densité démographique |
| `Taux de chômage au T4` | Taux de chômage au 4e trimestre |
| `Taux de pauvreté` | Pourcentage de population en situation de pauvreté |
| `Nombre de logements` | Total des logements |
| ... | Autres indicateurs relatifs au marché de l'immobilier |

### Caractéristiques
- **Séparateur** : Point-virgule (`;`)
- **En-tête** : Première ligne contient les noms de colonnes
- **Inférence de schéma** : Automatique (conversion des types numériques)
- **Géométries** : Données géographiques GeoJSON inclusescode_région

## 🚀 Utilisation

### Prérequis
- Python 3.8+
- Apache Spark 3.0+ (avec support PySpark)
- Java 8+ (dépendance de Spark)

### Installation des dépendances

```bash
pip install pyspark
```

### Exécution du script

```bash
# Depuis le répertoire racine du projet
spark-submit scripts/csv_to_json.py

# Ou en utilisant Python directement
python scripts/csv_to_json.py
```

### Paramètres configurables

Modifiez les constantes au début du script `csv_to_json.py` :

```python
INPUT_CSV = "/data/logements_regions.csv"   # Chemin d'entrée
OUT_JSON  = "/output/logements_regions_json" # Chemin de sortie
```

## 🔄 Processus de transformation

```
1. Initialisation Spark
   ↓
2. Lecture CSV
   - Séparateur: ;
   - Inférence de schéma
   ↓
3. Affichage diagnostiques (schéma brut, aperçu, nombre de lignes)
   ↓
4. Nettoyage des données
   - Normalisation des noms de colonnes
   - Suppression des doublons
   ↓
5. Affichage diagnostiques (schéma nettoyé, aperçu final)
   ↓
6. Export JSON
   - Partitionnement par Spark
   - Fichiers part-*.json
   ↓
7. Fermeture Spark
```

## 📈 Sortie attendue

Après exécution, le dossier `output/logements_regions_json/` contient :
- **`_SUCCESS`** : Fichier marqueur indiquant la réussite du traitement
- **`part-00000-*.json`** : Fichiers JSON avec les données transformées

### Exemple de structure JSON

```json
{
  "année_publication": 2023,
  "code_région": "84",
  "nom_region": "AUVERGNE-RHÔNE-ALPES",
  "nombre_habitants": 8113907,
  "densité_de_population_au_km2": 116,
  "taux_de_chômage_au_t4_en_%": 6.1,
  "taux_de_pauvreté_en_%": 12.7,
  "nombre_de_logements": 4571047,
  ...
}
```

## 🛠️ Fonctions principales

### `clean_col(c: str) -> str`
Normalise les noms de colonnes :
- Supprime les espaces au début/fin
- Convertit en minuscules
- Remplace les espaces multiples par `_`
- Supprime tous les caractères spéciaux (garde uniquement `a-z0-9_`)

**Exemple :**
```
"Taux de  chômage*" → "taux_de_chômage"
```

## 📝 Logs et débogage

Le script affiche automatiquement :

```
SCHEMA BRUT
├─ année_publication: integer
├─ code_région: string
└─ ...

APERCU BRUT
├─ Affiche les 10 premières lignes non tronquées
└─ ...

NB LIGNES BRUT: [nombre]

SCHEMA NETTOYE
└─ Structure après nettoyage

APERCU NETTOYE
└─ Derniers 10 enregistrements nettoyés

NB LIGNES FINAL: [nombre]

JSON ECRIT DANS /output/logements_regions_json
```

## 🐳 Intégration Docker (optionnel)

Pour exécuter ce projet dans un conteneur Docker :

```dockerfile
FROM apache/spark:latest

WORKDIR /app

COPY scripts/ ./scripts/
COPY data/ ./data/

CMD ["spark-submit", "scripts/csv_to_json.py"]
```

```bash
docker build -t tp-spark-csv-json .
docker run -v $(pwd)/output:/app/output tp-spark-csv-json
```

## 🗄️ MongoDB (futurs développements)

Ce projet pourrait être étendu pour charger automatiquement les données JSON dans une base de données MongoDB pour :
- Requêtes plus complexes
- Indexation géographique
- Agrégations analytiques

## ✅ Validation des résultats

Pour vérifier l'exécution :

```bash
# Vérifier la présence du fichier de succès
ls -la output/logements_regions_json/_SUCCESS

# Visualiser un extrait du JSON généré
head -20 output/logements_regions_json/part-*.json | python -m json.tool
```

## 📚 Améliorations futures

- [ ] Support de multiples fichiers CSV en entrée
- [ ] Configuration par fichier (YAML/JSON)
- [ ] Validation du schéma
- [ ] Intégration MongoDB automatique
- [ ] Compression des fichiers de sortie (GZIP)
- [ ] Partition des données par années
- [ ] Tests unitaires
- [ ] Pipeline CI/CD avec GitHub Actions


---

**Dernière mise à jour :** Février 2026
