# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
from pyspark.sql.functions import col, from_unixtime

# 1. Testujemy zakres dla 8 marca
start_date = '2026-03-09'
end_date = '2026-03-10' 

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
response = requests.get(url)

if response.status_code == 200:
    features = response.json().get('features', [])
    if features:
        # Tworzymy DataFrame
        df = spark.read.json(sc.parallelize([json.dumps(f) for f in features]))
        
        # Wyciągamy ID, Magnitudę i formatujemy TIME
        df_with_time = df.select(
            col("id"),
            col("properties.mag").alias("magnituda"),
            col("properties.place").alias("lokalizacja"),
            # Dzielimy przez 1000 (ms -> s) i zmieniamy na datę
            from_unixtime(col("properties.time") / 1000).alias("dokladny_czas_wydarzenia")
        ).orderBy(col("dokladny_czas_wydarzenia").desc())
        
        print(f"DANE DLA ZAKRESU: {start_date} DO {end_date}")
        display(df_with_time)
    else:
        print(f"BRAK DANYCH dla zakresu {start_date} - {end_date}. To dowód, że w API jeszcze nic nie ma!")
else:
    print(f"Błąd API: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
