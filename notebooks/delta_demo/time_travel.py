# Databricks notebook source
from datetime import datetime, timedelta

from delta.tables import DeltaTable

silver_path = "/mnt/public/silver/generated"

# Display current silver
df = spark.read.load(silver_path)
display(df)

# COMMAND ----------
# Read and display for instance a 20 minutes old version. Any date or datetime would work
old_df = spark.read.option(
    "timestampAsOf", datetime.now() - timedelta(minutes=20)
).load("/mnt/public/silver/generated")
display(df)

# COMMAND ----------
# Check out the full history
dt = DeltaTable.forPath(spark, "/mnt/public/silver/generated")
history = dt.history()
display(history)
