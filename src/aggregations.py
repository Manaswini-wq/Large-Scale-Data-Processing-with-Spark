"""
Aggregation queries for taxi analytics.
Each function returns a DataFrame ready for BigQuery.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def hourly_stats(df: DataFrame) -> DataFrame:
    """Revenue, trips, and avg metrics per hour of day."""
    return (
        df.groupBy("pickup_date", "pickup_hour", "time_of_day")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.sum("passenger_count").alias("total_passengers"),
        )
        .withColumn("revenue_per_trip", F.col("total_revenue") / F.col("trip_count"))
        .orderBy("pickup_date", "pickup_hour")
    )


def zone_revenue(df: DataFrame) -> DataFrame:
    """Top revenue zones with route analysis."""
    return (
        df.groupBy("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount_with_tip").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
        )
        .filter(F.col("trip_count") >= 100)
        .orderBy(F.desc("total_revenue"))
    )


def daily_summary(df: DataFrame) -> DataFrame:
    """Daily summary with rolling 7-day averages."""
    daily = (
        df.groupBy("pickup_date", "is_weekend")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.countDistinct("PULocationID").alias("unique_pickup_zones"),
        )
    )

    window_7d = (
        Window
        .orderBy(F.col("pickup_date").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )

    return (
        daily
        .withColumn("rolling_7d_avg_revenue", F.avg("total_revenue").over(window_7d))
        .withColumn("rolling_7d_avg_trips", F.avg("trip_count").over(window_7d))
        .orderBy("pickup_date")
    )


def driver_patterns(df: DataFrame) -> DataFrame:
    """Analyze payment and tipping patterns by time and location."""
    return (
        df.groupBy("pickup_borough", "time_of_day", "is_weekend", "payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.avg("trip_distance").alias("avg_distance"),
            F.percentile_approx("fare_amount", 0.5).alias("median_fare"),
            F.percentile_approx("tip_percentage", 0.5).alias("median_tip_pct"),
        )
        .filter(F.col("trip_count") >= 50)
        .orderBy("pickup_borough", "time_of_day")
    )
