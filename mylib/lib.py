import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import concat_ws
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
)

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return True

def extract_csv(url, file_path, directory="data"):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(os.path.join(directory, file_path), "wb") as f:
            f.write(r.content)
    return os.path.join(directory, file_path)

def load_data(spark, data="data/births.csv"):
    """Load births data with schema definition"""
    schema = StructType([
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("date_of_month", IntegerType(), True),
                StructField("day_of_week", IntegerType(), True),
                StructField("births", IntegerType(), True)
            ])
    
    df = spark.read.option("header", "true").schema(schema).csv(data)
    return df

def query(spark, df, query, name='temp_table'):
    """Queries using Spark SQL"""
    df.createOrReplaceTempView(name)
    return spark.sql(query).show()

def describe(df):
    df.describe().toPandas().to_markdown()
    return df.describe().show()

def example_transform(df):
    # Combine year, month, and date_of_month into a single date string
    df = df.withColumn("full_date", concat_ws("-", df["year"], df["month"],
                                              df["date_of_month"]))

    # Classify day_of_week as weekend or weekday
    df = df.withColumn("day_type", 
                       when(df["day_of_week"].isin([1, 7]), 
                            "Weekend").otherwise("Weekday"))

    return df.show()