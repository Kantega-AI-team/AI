# Databricks notebook source
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

# Set the schema - inferring is ineficcient and
# does not succeed in detecting correct types
schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("value", DoubleType()),
        StructField("time", TimestampType()),
    ]
)

# Set locations
raw_path = "/mnt/public/raw/generated/dummy_*.csv"
bronze_path = "/mnt/public/bronze/generated/v1"
checkpoint_path = "/mnt/public/bronze/generated/checkpoints/v1"

# Initialize stream read
df = spark.readStream.format("csv").option("header", True).schema(schema).load(raw_path)

# Perform stream write. Will run until stopped, checking for new
# source files every 30 seconds.
df.writeStream.trigger(processingTime="30 seconds").option(
    "ignoreChanges", True
).outputMode("append").option("checkpointLocation", checkpoint_path).format(
    "delta"
).start(
    bronze_path
)

# To check out result - run display(spark.read.load(bronze_path))
