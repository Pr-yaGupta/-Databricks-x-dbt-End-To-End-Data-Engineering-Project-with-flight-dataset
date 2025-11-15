import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "silver_business"
)

def silver_business():

    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_flights"), ["flight_id"])\
            .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
            .join(dlt.readStream("silver_airports"), ["airport_id"])\
            .drop("modified_date")

    return df