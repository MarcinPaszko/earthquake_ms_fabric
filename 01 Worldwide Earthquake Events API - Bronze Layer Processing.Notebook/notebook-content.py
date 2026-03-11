# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bdbf4503-c32a-4eec-9ed2-9eba597eb802",
# META       "default_lakehouse_name": "earthquakes_lakehouse",
# META       "default_lakehouse_workspace_id": "a45fa00c-b9e8-4f15-aa49-7bcd252d7ee6",
# META       "known_lakehouses": [
# META         {
# META           "id": "bdbf4503-c32a-4eec-9ed2-9eba597eb802"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # <mark></mark>Worldwide Earthquake Events API - Bronze Layer Processing

# CELL ********************

import requests
import json
from pyspark.sql.functions import col, from_unixtime

# 1. Pobieranie danych
start_date = '2026-03-06'
end_date = '2024-03-06'
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
response = requests.get(url)

if response.status_code == 200:
    features = response.json().get('features', [])
    
    if features:
        # 2. Tworzymy DataFrame
        raw_df = spark.read.json(sc.parallelize([json.dumps(f) for f in features]))
        
        # 3. TRANSFORMACJA CZASU:
        # Dzielimy przez 1000, bo API daje milisekundy, a from_unixtime chce sekundy
        df_clean = raw_df.select(
            col("id"),
            col("properties.mag").alias("magnituda"),
            col("properties.place").alias("lokalizacja"),
            from_unixtime(col("properties.time") / 1000).alias("data_godzina_utc")
        )
        
        # 4. Wyświetlamy
        display(df_clean)
    else:
        print("Brak danych.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

