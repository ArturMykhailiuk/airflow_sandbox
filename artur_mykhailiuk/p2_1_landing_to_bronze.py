from pyspark.sql import SparkSession
import os

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

# Завантаження файлів з FTP-сервера
ftp_urls = {
    "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.txt",
    "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.txt"
}

for table, url in ftp_urls.items():
    # Читання CSV-файлу
    df = spark.read.csv(url, header=True, inferSchema=True)
    
    # Збереження у форматі Parquet
    df.write.mode("overwrite").parquet(f"bronze/{table}")

spark.stop()