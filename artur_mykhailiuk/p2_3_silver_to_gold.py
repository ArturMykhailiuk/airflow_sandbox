from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Читання таблиць з папки silver
athlete_bio_df = spark.read.parquet("silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

# Об'єднання таблиць за колонкою athlete_id
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Обчислення середніх значень
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

# Збереження у форматі Parquet
aggregated_df.write.mode("overwrite").parquet("gold/avg_stats")

spark.stop()