import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = "trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta")\
                .load("/Volumes/databricks_flight_catalog/bronze/bronzevolume/flights/data/")
    df = df.withColumn("flight_date", to_date(col("flight_date")))\
        .withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)