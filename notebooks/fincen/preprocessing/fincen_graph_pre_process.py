# Databricks notebook source
# MAGIC %md
# MAGIC ##Constructing a Fincen GraphX graph
# MAGIC We will construct a graph by extracting descriptions of vertices and edges

# COMMAND ----------

from functools import reduce

from graphframes import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, concat, lit, monotonically_increasing_id, when

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input data
# MAGIC Read fincen data frame in parque format

# COMMAND ----------

fincen = spark.read.format("delta").load("/mnt/public/clean/fincen")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vertices manipulation
# MAGIC Transforming the fincen data to create a vertices data frame.
# MAGIC Two symmetric datasets are required: **senders** and **beneficiaries**.
# MAGIC We highlight now the steps for creating the senders dataset:
# MAGIC 1. Selecting _sender_ and _sender_country_
# MAGIC 2. Rename _sender_ and _sender_country_ as _bank_ and _country_, respectively
# MAGIC 3. Keep only unique and distinct rows
# MAGIC
# MAGIC The final **vertices** dataFrame will be made of
# MAGIC - _bank_
# MAGIC - _country_
# MAGIC - _id_

# COMMAND ----------


senders = (
    fincen.select("sender", "sender_country")
    .withColumnRenamed("sender", "bank")
    .withColumnRenamed("sender_country", "country")
    .dropDuplicates()
)
beneficiaries = (
    fincen.select("beneficiary", "beneficiary_country")
    .withColumnRenamed("beneficiary", "bank")
    .withColumnRenamed("beneficiary_country", "country")
    .dropDuplicates()
)

# Create a vertices data frame by joining senders and beneficiaries
# The ID-column is set as the row-index
vertices = (
    senders.unionByName(beneficiaries)
    .dropDuplicates()
    .withColumn("id", monotonically_increasing_id())
)

# vertices.count()  #1655 vertices:

vertices.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Edge manipulation
# MAGIC - In order to run the GNN using features attached to the edges we need first to link the edges to the nodes by using the same ID
# MAGIC - The departing node will be called **src** while the destination node will be called **dst**.
# MAGIC - Each node will be uniquely identified by the name of the bank and the county where the bank is located
# MAGIC - We sum all the edges sharing the same src and dst. This  will aggregate all the different relations btw the same two banks, having the same direction (A-->B) og (B-->A)

# COMMAND ----------

# Create an edge dataframe from the basis of the fincen data. Create id values such that sources and destinations match ids in the vertices table.
edges = fincen.select(
    "sender",
    "sender_country",
    "beneficiary",
    "beneficiary_country",
    "amount_transactions",
    "number_transactions",
).withColumn("relation_type", lit("transfer"))

# Join the edge and the vertices data frames to include correct vertices id references.
# The column referring to the sender is labelled "src", while the beneficiary is labeled "dst"
# The final selection constructs a data frame consisting of src, dst, relation_type, amount_transactions, and number_transactions
edges = (
    edges.join(
        vertices.withColumnRenamed("id", "src"),
        [edges.sender_country == vertices.country, edges.sender == vertices.bank],
    )
    .drop("bank", "country")
    .join(
        vertices.withColumnRenamed("id", "dst"),
        [
            edges.beneficiary_country == vertices.country,
            edges.beneficiary == vertices.bank,
        ],
    )
    .select("src", "dst", "relation_type", "amount_transactions", "number_transactions")
)

# Merge all the edges between the same banks in one single edge - amount_transactions and number_transactions will be summed up
# now between two banks there will be max 2 edges (A-->B and B-->A). This simplifies the construction of edges features
edges = (
    edges.groupBy("src", "dst")
    .agg({"amount_transactions": "sum", "number_transactions": "sum"})
    .withColumnRenamed("sum(amount_transactions)", "amount_transactions")
    .withColumnRenamed("sum(number_transactions)", "number_transactions")
)


edges.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph Frame
# MAGIC Constructing a graph frame on the basis of the vertices and edges data frames.
# MAGIC
# MAGIC We will not use the graph further in this notebook, but do this step to ensure that we are able to construct a graph on the basis of the data transformations above.

# COMMAND ----------

g = GraphFrame(
    vertices,
    edges,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Storage
# MAGIC Stores vertices and edges data as delta parguet

# COMMAND ----------

g.vertices.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
    "/mnt/public/clean/fincen-graph/vertices"
)
g.edges.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
    "/mnt/public/clean/fincen-graph/edges"
)
