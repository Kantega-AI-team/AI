# Databricks notebook source
# MAGIC %md
# MAGIC # Fincen - Detecting communities and major players
# MAGIC
# MAGIC In this notebook we will use the pagerank algorithm to score and rank the banks that are most central in the overall flow of bank transfers.
# MAGIC Further, we will use label propagation to identify subcomminities within the graph.

# COMMAND ----------

from functools import reduce

from graphframes import *
from pyspark.sql.functions import col, desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading graph data
# MAGIC Building a fincen graph by loading the cleaned version from delta lake

# COMMAND ----------

vertices = spark.read.load("/mnt/public/clean/fincen-graph/vertices")
edges = spark.read.load("/mnt/public/clean/fincen-graph/edges")

g = GraphFrame(
    vertices,
    edges,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagerank
# MAGIC Identify important vertices in a graph based on connections.

# COMMAND ----------

# Calculate the pagerank score for each bank
g = g.pageRank(resetProbability=0.15, tol=0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label propagation
# MAGIC Run static Label Propagation Algorithm for detecting communities in networks.
# MAGIC
# MAGIC Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the most frequent community affiliation of incoming messages.
# MAGIC
# MAGIC LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

# COMMAND ----------

communities = g.labelPropagation(maxIter=5).withColumnRenamed("label", "community_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display results

# COMMAND ----------

display(communities)
