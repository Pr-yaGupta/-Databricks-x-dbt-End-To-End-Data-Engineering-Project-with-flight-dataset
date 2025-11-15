import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = "trans_airports"
)

def trans_airports():
    df = spark.readStream.format("delta")\
                .load("/Volumes/databricks_flight_catalog/bronze/bronzevolume/airports/data/")
    df = df.withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)