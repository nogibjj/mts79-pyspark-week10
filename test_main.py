import pytest
from pyspark.sql import SparkSession
from mylib.lib import (
    extract_csv,
    describe,
    load_data,
    query,
)
import os


@pytest.fixture(scope="module")
def spark():
    # Start a Spark session
    spark = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()
    yield spark
    # Stop the Spark session after tests
    spark.stop()


def test_start_spark(spark):
    # Test Spark session is started
    assert spark is not None


def test_extract_csv():
    output_path = extract_csv(
        url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/births/US_births_2000-2014_SSA.csv",
        file_path="test_births.csv",
        directory="data",
    )
    assert os.path.exists(output_path), "CSV file was not extracted as expected."


def test_load_data(spark):
    # Try loading data and perform a simple check
    df = load_data(spark, data="data/test_births.csv")
    assert df is not None, "Dataframe is None; load_data failed."
    assert df.count() > 0, "Dataframe is empty; load_data did not load data correctly."
    assert "births" in df.columns, "Expected column 'births' not found in DataFrame."
    assert "year" in df.columns, "Expected column 'year' not found in DataFrame."
    assert "month" in df.columns, "Expected column 'month' not found in DataFrame."


def test_query(spark):
    df = load_data(spark, data="data/test_births.csv")
    # Run a basic query to test SQL functionality
    res = query(
        spark,
        df,
        query="SELECT year, month, SUM(births) as total_births FROM temp_table GROUP BY year, month",
    )
    assert res is None, "Query function did not execute as expected."


def test_describe(spark):
    df = load_data(spark, data="data/test_births.csv")
    res = describe(df)
    assert res is None, "Describe function did not execute as expected."
