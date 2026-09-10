"""
Demonstrates Spark optimization techniques.
Run these to compare execution plans and performance.
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def compare_join_strategies(spark: SparkSession, trips_df: DataFrame, zones_df: DataFrame):
    """Compare broadcast join vs shuffle join performance."""

    print("=" * 60)
    print("JOIN STRATEGY COMPARISON")
    print("=" * 60)

    # 1. Shuffle Hash Join (default for large tables)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    start = time.time()
    shuffle_result = trips_df.join(zones_df, trips_df.PULocationID == zones_df.LocationID)
    shuffle_count = shuffle_result.count()
    shuffle_time = time.time() - start
    print(f"Shuffle Join: {shuffle_count} rows in {shuffle_time:.2f}s")
    shuffle_result.explain(mode="formatted")

    # 2. Broadcast Join (small table broadcast to all executors)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
    start = time.time()
    broadcast_result = trips_df.join(
        F.broadcast(zones_df), trips_df.PULocationID == zones_df.LocationID
    )
    broadcast_count = broadcast_result.count()
    broadcast_time = time.time() - start
    print(f"Broadcast Join: {broadcast_count} rows in {broadcast_time:.2f}s")
    broadcast_result.explain(mode="formatted")

    print(f"\nSpeedup: {shuffle_time / broadcast_time:.2f}x faster with broadcast")


def demonstrate_partitioning(df: DataFrame):
    """Show impact of repartitioning on query performance."""

    print("=" * 60)
    print("PARTITIONING IMPACT")
    print("=" * 60)

    # Query without repartition
    start = time.time()
    df.filter(F.col("pickup_month") == 1).groupBy("pickup_hour").count().collect()
    no_repart_time = time.time() - start
    print(f"Without repartition: {no_repart_time:.2f}s")

    # Query with repartition by month
    repartitioned = df.repartition(12, "pickup_month")
    start = time.time()
    repartitioned.filter(F.col("pickup_month") == 1).groupBy("pickup_hour").count().collect()
    repart_time = time.time() - start
    print(f"With repartition by month: {repart_time:.2f}s")


def demonstrate_caching(df: DataFrame):
    """Show impact of caching on repeated queries."""

    print("=" * 60)
    print("CACHING IMPACT")
    print("=" * 60)

    # First query without cache
    start = time.time()
    df.groupBy("pickup_borough").agg(F.avg("fare_amount")).collect()
    first_time = time.time() - start
    print(f"First query (no cache): {first_time:.2f}s")

    # Cache and rerun
    df.cache()
    df.count()  # materialize cache
    start = time.time()
    df.groupBy("pickup_borough").agg(F.avg("fare_amount")).collect()
    cached_time = time.time() - start
    print(f"Second query (cached): {cached_time:.2f}s")
    print(f"Speedup: {first_time / cached_time:.2f}x")

    df.unpersist()


def show_execution_plan(df: DataFrame, label: str = "Query"):
    """Print physical and logical execution plans."""
    print(f"\n{'=' * 60}")
    print(f"EXECUTION PLAN: {label}")
    print(f"{'=' * 60}")
    df.explain(mode="extended")
