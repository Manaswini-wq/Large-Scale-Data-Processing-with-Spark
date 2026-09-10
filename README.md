# Large-Scale Batch Processing with PySpark

Processes 50GB+ of NYC Yellow Taxi trip data using PySpark, demonstrating
complex transformations, broadcast joins, window functions, and Spark
performance optimization — with results loaded into partitioned BigQuery tables.

## Architecture

```
NYC Taxi Data (Parquet) → PySpark Pipeline → BigQuery Analytics Tables
        │                       │
   6 months data          Clean → Enrich → Aggregate
   ~50GB, 25M+ rows       Broadcast Join (Zones)
                           Window Functions (7d Rolling)
                           Partitioning + Caching
```

## Key Skills Demonstrated

- Processing 25M+ rows with PySpark
- Broadcast joins vs shuffle joins (with benchmarks)
- Window functions (rolling 7-day averages)
- Data partitioning and caching strategies
- Execution plan analysis (explain)
- BigQuery integration with partitioned tables
- Dataproc cluster deployment via Terraform
- Comprehensive unit tests

## Quick Start

```bash
pip install -r requirements.txt

# Download data (~8GB)
python -m src.ingest

# Run full pipeline
python -m src.pipeline

# Run optimization benchmarks
python -m src.optimize

# Run tests
pytest tests/ -v
```

## Output Tables

| Table           | Description                                   |
|-----------------|-----------------------------------------------|
| trips_cleaned   | 25M+ cleaned and enriched trip records        |
| hourly_stats    | Revenue and trip metrics per hour             |
| zone_revenue    | Top revenue routes by pickup/dropoff zone     |
| daily_summary   | Daily aggregates with 7-day rolling averages  |
| driver_patterns | Payment and tipping patterns by time/location |
