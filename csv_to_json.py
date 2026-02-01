import re
from pyspark.sql import SparkSession

INPUT_CSV = "/data/logements_regions.csv"
OUT_JSON  = "/output/logements_regions_json"

def clean_col(c: str) -> str:
    c = c.strip().lower()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^a-z0-9_]", "", c)
    return c

spark = SparkSession.builder.appName("TP_Docker_Spark_CSV_to_JSON").getOrCreate()

# 1) Lire le CSV local (toute la base)
df_raw = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .option("sep", ";")
          .csv(INPUT_CSV))

print("SCHEMA BRUT")
df_raw.printSchema()
print("APERCU BRUT")
df_raw.show(10, truncate=False)
print("NB LIGNES BRUT", df_raw.count())

# 2) Nettoyage colonnes et doublons (de la base)
df = df_raw.toDF(*[clean_col(c) for c in df_raw.columns]).dropDuplicates()

print("SCHEMA NETTOYE")
df.printSchema()
print("APERCU NETTOYE")
df.show(10, truncate=False)
print("NB LIGNES FINAL", df.count())

# 3) Export JSON (Spark crée un dossier avec part-*.json)
(df.write
 .mode("overwrite")
 .json(OUT_JSON))

print("JSON ECRIT DANS", OUT_JSON)

spark.stop()
