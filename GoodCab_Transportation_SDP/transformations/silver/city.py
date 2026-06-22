from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name = "transportation.silver.city",
    comment = "Cleaned and standardized dimensions",
    table_properties = {
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city")
    df_silver = df_bronze.select(col("city_id"), col("city_name"), col("ingest_datetime").alias("bronze_ingest_timestamp"))

    df_silver = df_silver.withColumn("silver_processed_timestamp", current_timestamp())

    return df_silver