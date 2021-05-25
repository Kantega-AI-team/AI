# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col

dt = DeltaTable.forPath(spark, "/mnt/public/silver/generated/")

# Display the dataframe
display(dt.toDF())

# COMMAND ----------

# To delete, simply run the delete command on the delta table
dt.delete(col("id") == 2)

# Display the dataframe again
display(dt.toDF())

# COMMAND ----------
# MAGIC %md  note - the data will still be there - you just mark those rows as deleted
# MAGIC To fully delete, a vacuum is needed.
# MAGIC Check the GDPR specifics [here](https://docs.microsoft.com/en-us/azure/databricks/security/privacy/gdpr-delta)
