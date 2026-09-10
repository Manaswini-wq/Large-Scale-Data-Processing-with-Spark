"""
Main pipeline orchestrator.
Runs the full batch pipeline: read → clean → transform → aggregate → write.
"""

import time
from src.utils import load_config, get_env, create_spark_session
from src.transform import clean_trips, add_time_features, add_trip_metrics, enrich_with_zones
from src.aggregations import hourly_stats, zone_revenue, daily_summary, driver_patterns


def write_to_bigquery(df, project, dataset, table, bucket, mode="overwrite"):
    (
        df.write
        .format("bigquery")
        .option("table", f"{project}.{dataset}.{table}")
        .option("temporaryGcsBucket", bucket)
        .mode(mode)
        .save()
    )
    print(f"  Written to {dataset}.{table}")


def run():
    config = load_config()
    project = get_env("GCP_PROJECT_ID")
    spark = create_spark_session(config)
    dataset = config["bigquery"]["dataset"]
    bucket = config["gcp"]["bucket"]
    tables = config["bigquery"]["tables"]

    pipeline_start = time.time()

    # --- STEP 1: Read raw data ---
    print("\n[1/6] Reading raw parquet data...")
    raw_df = spark.read.parquet("data/raw/yellow_tripdata_*.parquet")
    raw_count = raw_df.count()
    print(f"  Loaded {raw_count:,} raw records")

    # --- STEP 2: Clean ---
    print("\n[2/6] Cleaning data...")
    cleaned = clean_trips(raw_df)
    clean_count = cleaned.count()
    print(f"  {clean_count:,} records after cleaning ({raw_count - clean_count:,} removed)")

    # --- STEP 3: Add features ---
    print("\n[3/6] Adding time features and trip metrics...")
    enriched = add_trip_metrics(add_time_features(cleaned))

    # --- STEP 4: Zone enrichment (broadcast join) ---
    print("\n[4/6] Enriching with zone lookup (broadcast join)...")
    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/raw/taxi_zone_lookup.csv")
    )
    print(f"  Zone lookup: {zones.count()} zones (broadcast size: ~10KB)")
    final = enrich_with_zones(enriched, zones)

    # Cache for multiple aggregations
    final.cache()
    final_count = final.count()
    print(f"  Final dataset: {final_count:,} records")

    # --- STEP 5: Aggregations ---
    print("\n[5/6] Running aggregations...")

    print("  Computing hourly stats...")
    hourly = hourly_stats(final)
    write_to_bigquery(hourly, project, dataset, tables["hourly_stats"], bucket)

    print("  Computing zone revenue...")
    zones_rev = zone_revenue(final)
    write_to_bigquery(zones_rev, project, dataset, tables["zone_revenue"], bucket)

    print("  Computing daily summary...")
    daily = daily_summary(final)
    write_to_bigquery(daily, project, dataset, tables["daily_summary"], bucket)

    print("  Computing driver patterns...")
    patterns = driver_patterns(final)
    write_to_bigquery(patterns, project, dataset, tables["driver_patterns"], bucket)

    # --- STEP 6: Write cleaned trips (partitioned) ---
    print("\n[6/6] Writing cleaned trips to BigQuery (partitioned by date)...")
    write_to_bigquery(final, project, dataset, tables["trips_cleaned"], bucket)

    final.unpersist()
    total_time = time.time() - pipeline_start
    print(f"\nPipeline complete in {total_time:.1f}s")
    print(f"  Input:  {raw_count:,} records")
    print(f"  Output: {final_count:,} records")
    print(f"  Tables: {len(tables)} written to {dataset}")

    spark.stop()


if __name__ == "__main__":
    run()
