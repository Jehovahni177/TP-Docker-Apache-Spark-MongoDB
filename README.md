# TP PySpark & MongoDB - Pipeline OpenData avec Docker

Mise en place d'un pipeline complet de collecte, structuration, transformation et stockage de données OpenData.

---

##  1. Présentation générale du projet

Ce projet met en œuvre à l'aide de **PySpark** un processus complet de traitement de données OpenData.

Il repose sur l'utilisation conjointe de :
- **Docker** : pour la conteneurisation
- **Apache Spark** : pour le traitement distribué
- **MongoDB** : pour la persistance flexible

---

##  2. Objectifs pédagogiques

-  Créer des collections MongoDB pour l'organisation de la donnée
-  Écrire des processus PySpark d'import et de transformation
-  Stocker les données dans MongoDB via JSON

---

##  3. Pré-requis techniques

- Docker Desktop installé et lancé
- Windows avec PowerShell
- Accès Internet

### Vérification de Docker

\`powershell

docker --version

docker ps

\`

---

##  4. Structure du projet

\`

C:\tp_docker_spark_mongo

 data/           Données sources (CSV)

 scripts/        Scripts PySpark

 output/         Données transformées (JSON)

\`

### Création de la structure

\`powershell

mkdir C:\tp_docker_spark_mongo

cd C:\tp_docker_spark_mongo

mkdir data, output, scripts

\`

### Dépôt du fichier CSV

Le fichier **logements_regions.csv** (OpenData CDC) doit être placé ici :

\`

C:\tp_docker_spark_mongo\data\logements_regions.csv

\`

### Vérification

\`powershell

Get-Item .\data\logements_regions.csv

Get-Content .\data\logements_regions.csv -TotalCount 3

\`

---

##  5. Mise en place de l'environnement Docker

### Création d'un réseau Docker

\`bash

docker network create tp-net

\`

### Lancement de MongoDB

\`bash

docker pull mongo:latest

docker run -d --name mongo-tp --network tp-net -p 27017:27017 mongo:latest

\`

### Vérification de MongoDB

\`bash

docker ps

\`

**Test dans le conteneur :**

\`bash

docker exec -it mongo-tp mongosh

\`

**Dans mongosh :**

\`javascript

show dbs

exit

\`

### Lancement d'Apache Spark

\`bash

docker pull apache/spark:3.5.6

\`


\`bash

docker run -it --name spark-tp --network tp-net \
  -v C:\tp_docker_spark_mongo\data:/data \
  -v C:\tp_docker_spark_mongo\output:/output \
  -v C:\tp_docker_spark_mongo\scripts:/scripts \
  -e SPARK_HOME=/opt/spark \
  -e PATH=/opt/spark/bin:/opt/spark/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  apache/spark:3.5.6 bash

\`

### Vérification de Spark

\`bash

ls /opt/spark/bin

\`

---

##  6. Tests de fonctionnement Spark

### Spark Shell (Scala)

\`bash

/opt/spark/bin/spark-shell

\`

**Quitter :**

\`scala

:q

\`

### PySpark interactif

\`bash

/opt/spark/bin/pyspark

\`

**Test rapide :**

\`python

spark.range(10).show()

exit()

\`

### Vérification de l'accès au fichier CSV

\`bash

ls -l /data

head -n 3 /data/logements_regions.csv

exit

\`

---

##  7. Traitement PySpark : CSV  JSON

### Description du processus

Le script PySpark effectue :

1. Lecture d'un fichier CSV OpenData
2. Inférence automatique du schéma
3. Nettoyage et normalisation des colonnes
4. Suppression des doublons
5. Export en JSON

### Création du script

\`powershell

notepad C:\tp_docker_spark_mongo\scripts\csv_to_json.py

\`

**Contenu du script :**

\`python

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV_to_JSON").getOrCreate()

df = spark.read.csv("/data/logements_regions.csv", header=True, inferSchema=True)

# Nettoyage des noms de colonnes
df = df.toDF(*[col.lower().replace(" ", "_").replace("-", "_") for col in df.columns])

# Suppression des doublons
df = df.dropDuplicates()

# Export JSON
df.coalesce(1).write.mode("overwrite").json("/output/logements_regions_json")

print("Transformation terminée")

spark.stop()

\`

### Exécution du script

**Redémarrage du conteneur Spark :**

\`bash

docker ps -a

docker start spark-tp

docker ps

docker exec -it spark-tp bash

\`

**Vérification du script :**

\`bash

ls -l /scripts

\`

**Exécution :**

\`bash

/opt/spark/bin/spark-submit /scripts/csv_to_json.py

\`

**Vérification des fichiers générés :**

\`bash

ls -lah /output/logements_regions_json

head -n 20 /output/logements_regions_json/part-*.json

exit

\`

---

##  8. Vérification côté Windows

\`powershell

dir C:\tp_docker_spark_mongo\output\logements_regions_json

Get-Content C:\tp_docker_spark_mongo\output\logements_regions_json\part-*.json -TotalCount 3

\`

---

##  9. Modélisation et stockage dans MongoDB

MongoDB a été choisi car :

-  Nativement adapté aux formats JSON
-  Grande flexibilité de schéma
-  Exploitation simple (requêtes, agrégations, indexation)

### Création de la collection

\`bash

docker exec -it mongo-tp mongosh

\`

**Dans mongosh :**

\`javascript

use tp_logements

db.createCollection("logements_regions")

show collections

exit

\`

### Importation des JSON vers MongoDB

**Copie des fichiers dans le conteneur :**

\`powershell

docker cp C:\tp_docker_spark_mongo\output\logements_regions_json mongo-tp:/data_json

\`

**Accès au conteneur MongoDB :**

\`bash

docker exec -it mongo-tp bash

\`

**Dans le bash du conteneur :**

\`bash

for f in /data_json/logements_regions_json/part-*.json; do
  mongoimport \
    --db tp_logements \
    --collection logements_regions \
    --file "\"
done

\`

### Vérification de l'importation

\`bash

mongosh

\`

**Dans mongosh :**

\`javascript

use tp_logements

db.logements_regions.countDocuments()

db.logements_regions.findOne()

exit

\`

**Quittez le conteneur :**

\`bash

exit

exit

\`

---

##  Résumé du pipeline

\`

CSV (OpenData)
    
[Docker Spark]  Nettoyage & Transformation
    
JSON (formaté)
    
[Docker MongoDB]  Stockage flexible
    
Collection NoSQL prête à l'exploitation

\`

---

** Données utilisées :** Logements régionaux (Caisse des Dépôts - data.gouv)

** Stack technique :** Docker + Spark 3.5.6 + MongoDB + PySpark

** Dernière mise à jour :** Février 2026
