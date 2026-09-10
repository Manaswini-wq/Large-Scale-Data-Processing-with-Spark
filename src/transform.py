"""
Core data transformations for NYC Taxi data.
Handles cleaning, enrichment, and derived column computation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def clean_trips(df: DataFrame) -> DataFrame:
    """Remove invalid records and standardize columns."""
    return (
        df
        # Drop nulls in critical fields
        .dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance", "fare_amount",
            "PULocationID", "DOLocationID"
        ])
        # Filter invalid values
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 200)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("fare_amount") < 1000)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("passenger_count") <= 6)
        # Remove trips with negative duration
        .filter(
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime") > 0
        )
    )


def add_time_features(df: DataFrame) -> DataFrame:
    """Extract time-based features from pickup datetime."""
    return (
        df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0)
        )
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6, 11), "morning")
            .when(F.col("pickup_hour").between(12, 16), "afternoon")
            .when(F.col("pickup_hour").between(17, 21), "evening")
            .otherwise("night")
        )
    )


def add_trip_metrics(df: DataFrame) -> DataFrame:
    """Compute derived trip metrics."""
    return (
        df
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60
        )
        .withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") / (F.col("trip_duration_min") / 60)
            ).otherwise(0)
        )
        .withColumn(
            "cost_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount") / F.col("trip_distance")
            ).otherwise(0)
        )
        .withColumn(
            "cost_per_minute",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("fare_amount") / F.col("trip_duration_min")
            ).otherwise(0)
        )
        .withColumn(
            "total_amount_with_tip",
            F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0))
            + F.coalesce(F.col("tolls_amount"), F.lit(0))
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                (F.col("tip_amount") / F.col("fare_amount")) * 100
            ).otherwise(0)
        )
        # Filter out unreasonable speeds
        .filter(F.col("speed_mph") < 100)
        .filter(F.col("trip_duration_min") < 300)
    )


def enrich_with_zones(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """Broadcast join with zone lookup for pickup and dropoff names."""
    pickup_zones = zones_df.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    dropoff_zones = zones_df.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )

    return (
        trips_df
        .join(F.broadcast(pickup_zones), "PULocationID", "left")
        .join(F.broadcast(dropoff_zones), "DOLocationID", "left")
    )
