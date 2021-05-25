# Databricks notebook source
from pyspark.sql.types import StructType

raw_path = "/mnt/public/raw/doccc/*.csv"
bronze_path = "/mnt/public/bronze/doccc/v1"
checkpoint_path = "/mnt/public/bronze/doccc/checkpoints/v1"

schema = {
    "type": "struct",
    "fields": [
        {"name": "_c0", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X1", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X2", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X3", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X4", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X5", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X6", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X7", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X8", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X9", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X10", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X11", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X12", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X13", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X14", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X15", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X16", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X17", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X18", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X19", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X20", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X21", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X22", "type": "string", "nullable": True, "metadata": {}},
        {"name": "X23", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Y", "type": "string", "nullable": True, "metadata": {}},
    ],
}

# COMMAND ----------

# Create bronze
# Since all fields contains explanation as well as header, treat as string.
spark.readStream.format("csv").schema(StructType.fromJson(schema)).option(
    "header", True
).option("delimiter", ";").load(raw_path).writeStream.format("delta").trigger(
    once=True
).option(
    "checkpointLocation", checkpoint_path
).start(
    bronze_path
)
