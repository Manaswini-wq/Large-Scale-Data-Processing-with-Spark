import pytest
from pyspark.sql import SparkSession
from datetime import datetime
from src.transform import clean_trips, add_time_features, add_trip_metrics


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    yield s
    s.stop()


def make_trip(spark, overrides=None):
    base = {
        "tpep_pickup_datetime": datetime(2023, 1, 15, 8, 30),
        "tpep_dropoff_datetime": datetime(2023, 1, 15, 9, 0),
        "trip_distance": 5.0,
        "fare_amount": 25.0,
        "tip_amount": 5.0,
        "tolls_amount": 0.0,
        "passenger_count": 2,
        "PULocationID": 161,
        "DOLocationID": 237,
        "payment_type": 1,
    }
    if overrides:
        base.update(overrides)
    return spark.createDataFrame([base])


def test_clean_removes_zero_distance(spark):
    df = make_trip(spark, {"trip_distance": 0.0})
    assert clean_trips(df).count() == 0


def test_clean_removes_negative_fare(spark):
    df = make_trip(spark, {"fare_amount": -10.0})
    assert clean_trips(df).count() == 0


def test_clean_removes_excessive_distance(spark):
    df = make_trip(spark, {"trip_distance": 250.0})
    assert clean_trips(df).count() == 0


def test_clean_keeps_valid_trip(spark):
    df = make_trip(spark)
    assert clean_trips(df).count() == 1


def test_time_features(spark):
    df = make_trip(spark)
    result = add_time_features(clean_trips(df)).collect()[0]
    assert result["pickup_hour"] == 8
    assert result["time_of_day"] == "morning"
    assert result["is_weekend"] == 0
    assert result["pickup_month"] == 1


def test_trip_metrics(spark):
    df = make_trip(spark)
    result = add_trip_metrics(add_time_features(clean_trips(df))).collect()[0]
    assert result["trip_duration_min"] == 30.0
    assert result["speed_mph"] == 10.0
    assert result["cost_per_mile"] == 5.0
    assert result["tip_percentage"] == 20.0


def test_zero_passengers_removed(spark):
    df = make_trip(spark, {"passenger_count": 0})
    assert clean_trips(df).count() == 0
