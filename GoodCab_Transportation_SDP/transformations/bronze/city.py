from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

SOURCE_PATH = "s3://goodcab-026090514775-ap-south-1-an/city"

@dp.materialized_view(
    name = "transportation.bronze.city",
    comment = "Raw City Data Processing",
    table_properties = {
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = spark.read.format("csv")\
                   .option("header", True)\
                   .option("inferSchema", True)\
                   .option("mode", "PERMISSIVE")\
                    .option("mergeSchema", "true")\
                    .option("columnNameOfCorruptRecord","_corrupt_record")\
                    .load(SOURCE_PATH)

    df = df.withColumn("file_name", col("_metadata.file_name"))\
           .withColumn("ingest_datetime", current_timestamp())

    return df