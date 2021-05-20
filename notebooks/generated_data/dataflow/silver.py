# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

bronze_path = "/mnt/public/bronze/generated/v1"
silver_path = "/mnt/public/silver/generated/"
checkpoint_path = "/mnt/public/silver/checkpoints/generated/"


def upsert_to_silver(df: DataFrame, epoch_id: int) -> None:
    # Drop duplicates WITHIN batch
    df = df.dropDuplicates(["id", "value"])

    if DeltaTable.isDeltaTable(spark, silver_path):
        # If the table exists - merge into silver
        # Only insert rows that contains a new combination
        # Of id and value.
        DeltaTable.forPath(spark, silver_path).alias("silver").merge(
            source=df.alias("updates"),
            condition="updates.id == silver.id AND updates.value==silver.value",
        ).whenNotMatchedInsertAll().execute()
    else:
        # If the table does not exist - write
        df.write.format("delta").save(silver_path)


# Initialize readstream
df = spark.readStream.format("delta").load("/mnt/public/bronze/generated/v1")

# Set up writestream. With trigger=once - it
# is only treating new files once at any
# stream call. Change the trigger to adjust
stream = (
    df.writeStream.foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint_path)
    .trigger(once=True)
)

# Start the stream
stream.start()
