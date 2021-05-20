# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Silver
# MAGIC 
# MAGIC - Timestamps (longform)
# MAGIC - Arrays
# MAGIC - Join
# MAGIC - Oversette booleans fra “T” og “F”-verdier
# MAGIC - Navngivning av kolonner

# COMMAND ----------

# Import Python libraries

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

# COMMAND ----------

# Utility functions


def rename_columns(df: DataFrame, name_mapping: dict, func: callable) -> DataFrame:

    """[Change column headers based on selected function]

    Parameters
    ----------
    df : DataFrame
        [Data whose columns headers will be renamed]
    name_mapping : dict
        [Vector with the original column headers the will be modified]
    func : callable
        [function to apply to the column headers that will be modified]

    Returns
    -------
    DataFrame
        [dataset passed by input, with updated column headers]
    """

    for col in df.columns:
        df = df.withColumnRenamed(col, func(col))
    for name in name_mapping:
        df = df.withColumnRenamed(name, name_mapping[name])
    return df


def camel_to_snake(s: list) -> list:

    """[Define function for camel to snake in column headers]

    Parameters
    ----------
    s : List
        [Vector with columns headers to modify]

    Returns
    -------
    List
        [Vector with modified headers]
    """

    return (
        "".join(["_" + c.lower() if c.isupper() else c for c in s])
        .lstrip("_")
        .replace("i_d", "id")
        .replace("c_d", "cd")
        .replace("d_t", "dt")
    )


def with_boolean_column(df: DataFrame, column: str) -> DataFrame:

    """[Set boolean type on columns with true / false values]

    Parameters
    ----------
    df : DataFrame
        [Data Frame to be modified]
    column : str
        [named of the clumn to be transformed in boolean type]

    Returns
    -------
    DataFrame
        [Input Data Frame, with the updated boolean columnn]
    """

    temp_col = "temp"
    final_name = f"{column}_bool"
    df = (
        df.withColumn(
            temp_col, when(col(column) == "T", True).when(col(column) == "F", False)
        )
        .drop(column)
        .withColumnRenamed(temp_col, final_name)
    )
    return df


def with_boolean_columns(df: DataFrame, columns: list) -> DataFrame:
    """[Repeat with_boolean_columns for required features]

    Parameters
    ----------
    df : DataFrame
        [Data Frame  to be transformed]
    columns : List
        [Vector with the headers of the columns to be set to boolean type ]

    Returns
    -------
    DataFrame
        [Input Data Frame, with updated boolean columns]
    """

    for column in columns:
        df = with_boolean_column(df, column)
    return df


# COMMAND ----------

basepath_bronze = "/mnt/public/bronze/ieee_fraud"
basepath_silver = "/mnt/public/silver/ieee_fraud"
identity_bronze_path = f"{basepath_bronze}/identity"
identity_silver_path = f"{basepath_silver}/identity"
transaction_silver_path = f"{basepath_silver}/transaction"
transaction_bronze_path = f"{basepath_bronze}/transaction"
identity_checkpoint_path = f"{basepath_silver}/checkpoints/identity/"
transaction_checkpoint_path = f"{basepath_silver}/checkpoints/transaction/"


# COMMAND ----------

# Read bronze spark df
df_identity = spark.read.load(identity_bronze_path)
df_transaction = spark.read.load(transaction_bronze_path)
df_transaction = df_transaction.sample(fraction=0.01).cache()

# COMMAND ----------

df_identity.display()

# COMMAND ----------


def write_identity_data(df_identity: DataFrame, batchId: int) -> None:

    """[Creating identity silver data frame]

    Parameters
    ----------
    df_identity : DataFrame
        [Bronze identity Data Frame]
    batchId : int
        [Id of the batch ]
    """

    identity_name_mapping = {
        "id_30": "operating_system",
        "id_31": "browser",
        "id_33": "display_resolution",
    }
    df_identity_silver = rename_columns(
        df_identity, identity_name_mapping, camel_to_snake
    )
    df_identity_silver = with_boolean_columns(
        df_identity_silver, ["id_35", "id_36", "id_37", "id_38"]
    )

    df_identity_silver.write.format("delta").save(identity_silver_path)


# COMMAND ----------


def write_transaction_data(df_transaction: DataFrame, batchId: int) -> None:
    """[Creating transaction silver data frame]

    Parameters
    ----------
    df_transaction : DataFrame
        [Bronze transaction Data Frame]
    batchId : int
        [Id of the Batch]
    """

    transaction_name_mapping = {
        "addr1": "billing_region",
        "addr2": "billing_country",
        "card4": "card_provider",
        "card6": "card_type",
        "p_emaildomain": "purchaser_email_domain",
        "r_emaildomain": "recipient_email_domain",
    }
    df_transaction_silver = rename_columns(
        df_transaction, transaction_name_mapping, camel_to_snake
    )
    df_transaction_silver = with_boolean_columns(
        df_transaction_silver, ["m1", "m2", "m3", "m5", "m6", "m7", "m8", "m9"]
    )

    df_transaction_silver.write.format("delta").save(transaction_silver_path)


# COMMAND ----------

# df_identity: setup stream from bronze to silver

# identity stream
spark.readStream.format("delta").load(identity_bronze_path).writeStream.foreachBatch(
    write_identity_data
).trigger(once=True).option("checkpointLocation", identity_checkpoint_path).start()


# COMMAND ----------

# df_transaction: setupstream from bronze to silver

# transaction stream
spark.readStream.format("delta").load(transaction_bronze_path).writeStream.foreachBatch(
    write_transaction_data
).trigger(once=True).option("checkpointLocation", transaction_checkpoint_path).start()
