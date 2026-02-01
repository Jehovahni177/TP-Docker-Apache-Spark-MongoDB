**TP PySpark & MongoDB - Mise en place d’un pipeline de données OpenData avec Docker**

1. Présentation générale du projet

Ce projet a objectif est de mettre en œuvre à l’aide de PySpark, un processus complet de collecte, structuration, transformation et stockage de données OpenData.

Le projet repose sur l’utilisation conjointe de Docker, Apache Spark et MongoDB afin de reproduire une chaîne de traitement de données proche des pratiques professionnelles en Data Engineering.

---

2. Objectifs pédagogiques (lien direct avec la consigne)

Conformément à la consigne fournie, ce TP vise à :

 a) Créer des tables et/ou collections permettant une organisation pertinente de la donnée,

 b) Ecrire des processus PySpark d’import, de transformation et d’enregistrement des données depuis des sources OpenData identifiées,

 c) Stocker la donnée dans un système de persistance adapté (MongoDB, option facultative mais réalisée).

---

3. Pré-requis techniques

 a) Docker Desktop installé et lancé,

 b) Système Windows avec PowerShell,

 c) Accès Internet (pour le téléchargement des images Docker).

**Vérification de Docker**

 docker --version

 docker ps

---

4. Structure du projet

C:\tp_docker_spark_mongo
│

├── data        → Données sources (CSV OpenData)

├── scripts     → Scripts PySpark

└── output      → Données transformées (JSON)

**Création de la structure du projet**

mkdir C:\tp_docker_spark_mongo

cd C:\tp_docker_spark_mongo

mkdir data, output, scripts

**Dépôt du fichier CSV ici :**

Le fichier OpenData logements_regions.csv est un dataset de la Caisse des Dépôts (CDC) Opendatasoft “Logements et logements sociaux dans les régions”. Il a été téléchargé via data.gouv.

C:\tp_docker_spark_mongo\data\logements_regions.csv

**Vérification**

Get-Item .\data\logements_regions.csv

Get-Content .\data\logements_regions.csv -TotalCount 3

---

5. Mise en place de l’environnement Docker

**Création d'un réseau Docker**

Un réseau dédié permettant la communication entre Spark et MongoDB.

docker network create tp-net

**Lancement de MongoDB via Docker**

docker pull mongo:latest

docker run -d --name mongo-tp --network tp-net -p 27017:27017 mongo:latest

**Vérification**

docker ps

 a) Test mongosh dans le conteneur :

docker exec -it mongo-tp mongosh

 b) Dans mongosh :

show dbs

exit

**Lancement d'Apache Spark via Docker**

docker pull apache/spark:3.5.6

docker run -it --name spark-tp --network tp-net `
  -v C:\tp_docker_spark_mongo\data:/data `
  -v C:\tp_docker_spark_mongo\output:/output `
  -v C:\tp_docker_spark_mongo\scripts:/scripts `
  -e SPARK_HOME=/opt/spark `
  -e PATH=/opt/spark/bin:/opt/spark/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin `
  apache/spark:3.5.6 bash

**Vérification**

ls /opt/spark/bin

---

6. Tests de fonctionnement Spark

**Spark Shell (Scala)**

/opt/spark/bin/spark-shell

*Quitter spark-shell*

:q

**PySpark**

/opt/spark/bin/pyspark

*Test rapide*

spark.range(10).show()

exit()

**Vérification de l'accès au fichier CSV(dans le conteneur Spark)**

ls -l /data

head -n 3 /data/logements_regions.csv

exit

---

7. Traitement PySpark : CSV → JSON

**Description du processus PySpark**

Un script PySpark est développé afin de :

 a) Lire un fichier CSV OpenData,

 b) Inférer automatiquement le schéma,

 c) Nettoyer et normaliser les noms de colonnes,

 d) Supprimer les doublons,

 e) Exporter les données transformées au format JSON.

Ce processus répond directement à l’exigence de la consigne concernant **l’écriture de processus PySpark d’import et d’enregistrement de la donnée**.

**Création du script sur Windows :**

notepad C:\tp_docker_spark_mongo\scripts\csv_to_json.py

**Exécution du script PySpark**

Redémarrage et accès au conteneur Spark.

*Vérification de l'état du conteneur*

docker ps -a

*Redémarrage du conteneur*

docker start spark-tp

docker ps

docker exec -it spark-tp bash

*Vérifions que le script est bien visible dans /scripts*

ls -l /scripts

*Exécution*

/opt/spark/bin/spark-submit /scripts/csv_to_json.py

*Vérification des fichiers générés*

ls -lah /output/logements_regions_json

head -n 20 /output/logements_regions_json/part-*.json

exit

---

8. Vérification côté Windows de la génération des JSON

dir C:\tp_docker_spark_mongo\output\logements_regions_json

Get-Content C:\tp_docker_spark_mongo\output\logements_regions_json\part-*.json -TotalCount 3

---

9. Modélisation et stockage dans MongoDB

MongoDB a été choisi car :

 a) Il est nativement adapté aux formats JSON produits par Spark,

 b) Il offre une grande flexibilité de schéma cohérente avec des données OpenData,

 c) Il permet une exploitation ultérieure simple (requêtes, agrégations, indexation).

**Création de la collection MongoDB**

docker exec -it mongo-tp mongosh

*Dans mongosh :*

use tp_logements

db.createCollection("logements_regions")

show collections

exit

**Importation des fichiers JSON vers MongoDB**

*Copie des fichiers dans le conteneur MongoDB*

docker cp C:\tp_docker_spark_mongo\output\logements_regions_json mongo-tp:/data_json

*Importation de tous les part-*.json dans MongoDB*

docker exec -it mongo-tp bash

*Dans le bash du conteneur Mongo :*

for f in /data_json/logements_regions_json/part-*.json; do
  mongoimport \
    --db tp_logements \
    --collection logements_regions \
    --file "$f"
done

*Vérification de l'importation*

mongosh

use tp_logements

db.logements_regions.countDocuments()

db.logements_regions.findOne()

exit


exit
