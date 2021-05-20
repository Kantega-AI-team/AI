# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

bronze_path = "/mnt/public/bronze/doccc/v1"
silver_meta_path = "/mnt/public/silver/doccc/field_descriptions"
silver_data_path = "/mnt/public/silver/doccc/doccc"
checkpoint_path = "/mnt/public/silver/doccc/checkpoints/"

# COMMAND ----------


def write_data_and_meta(df: DataFrame, batchId: int) -> None:
    meta = df.filter(col("_c0") == "ID")

    meta.write.format("delta").save(silver_meta_path)

    data = df.filter(col("_c0") != "ID")

    def data_recast(df: DataFrame) -> DataFrame:
        for column in df.columns:
            df = df.withColumn(column, col(column).cast(IntegerType()))
        return df.withColumnRenamed("_c0", "ID")

    data_recast(data).write.format("delta").save(silver_data_path)


# COMMAND ----------

spark.readStream.format("delta").load(bronze_path).writeStream.foreachBatch(
    write_data_and_meta
).trigger(once=True).option("checkpointLocation", checkpoint_path).start()
