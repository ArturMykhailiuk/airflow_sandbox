from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

# Функція для чистки тексту
def clean_text(df):
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

# Таблиці для обробки
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    # Читання таблиці з папки bronze
    df = spark.read.parquet(f"bronze/{table}")
    
    # Чистка тексту
    df = clean_text(df)
    
    # Дедублікація рядків
    df = df.dropDuplicates()
    
    # Збереження у форматі Parquet
    df.write.mode("overwrite").parquet(f"silver/{table}")

spark.stop()