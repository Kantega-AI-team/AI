# Databricks notebook source

import random
import string
from datetime import datetime

from pyspark.sql.functions import col, lit, when


def generate_csv(nrows: int, path: str = "/mnt/public/raw/generated") -> None:
    """
    Generate raw data with three columns: id, time and value
    Csv name: dummy_{3 random characters}.csv
    """
    asif = (
        spark.range(nrows * 2)
        # Creating some duplicates
        .unionByName(nrows * 2).withColumn(
            "value", when(col("id") < 3, lit(1)).otherwise(lit(0))
        )
        # Add a timestamp
        .withColumn("time", lit(datetime.now()))
        # Since doubled by union, sample half
        .sample(0.5)
        # Nicer file naming w/pandas - only for small datasets
        .toPandas()
    )
    dbutils.fs.mkdirs(path)
    # Generating a random string for file naming
    randstring = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    asif.to_csv("/dbfs{}/dummy_{}.csv".format(path, randstring), index=False)


# COMMAND ----------

# Generate by calling generate_csv()
# For delete and timetravel demo purposes - run first with lower number of rows
# and then higher number of rows to make sure silver is updated with several versions

generate_csv(nrows=5)
