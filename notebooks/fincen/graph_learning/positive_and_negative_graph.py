# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Positive and negative graph for Link Prediction
# MAGIC
# MAGIC Link prediction models are  Graph based ML methods whose goal is to predict the existence of a link (edge) between two nodes. These models exploit the ebmeddings representation of the graph obtained through the GNN models for the computation of a _score function_, predicting the existence of links between nodes.
# MAGIC
# MAGIC The link prediction task is performed as a supervised, binary classification problem. In order to train the model we need observations from both _sides_ of the outcome space; the successes (1's) and failures (0's). For Graph Neural Networks this transaltes to the observations  of instances (edges) having value 0 (non-existing) and 1 (existing). The instances having value  1 are the edges from the actual observed graph while the instances with value 0 must be retrieved from it. The procedure to obtain the so called  _negative example_ is strightforward: create a graph with the same nodes as the observed one (called _positive example_) but with edges connecting only pairs of nodes previously un-connected. Both graphs share the same nodes (hence the features). The embeddings learned from the graph are used for the computation of the edge score $$ y_{u,v} = \phi( h_u,h_v) $$   between any node _u_ and _v_. Function \\( \phi(\cdot) \\)  encourages higher scores between nodes actually connected by an edge than between randomly selected nodes.
# MAGIC  Thanks to the positive and negative exmaples, the parameter estimation process is carried out as usual by mean of a loss function taking as input both positive and negative edges.
# MAGIC
# MAGIC  In this tutorial we describe the procedure for creating the positive and negative examples using the FinCEN graph. Further, we split both examples in train and test set so to be ready to train and validate the chosen models.

# COMMAND ----------

# import required libraries

import itertools
import os

import dgl
import dgl.function as fn
import numpy as np
import pandas as pd
import scipy.sparse as sp
import torch as th
import torch.nn as nn
import torch.nn.functional as F
from dgl.data import DGLDataset
from dgl.data.utils import load_graphs, save_graphs
from matplotlib import pyplot
from sklearn.metrics import roc_auc_score, roc_curve

# COMMAND ----------

# load the FinCEN graph

fincen_graph = load_graphs("/dbfs/mnt/public/clean/fincen-graph/fincen_dgl")[0][0]

fincen_graph  # short summary of the graph

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Train and test set for the positive example

# COMMAND ----------

# MAGIC %md
# MAGIC First we split the positive example, hence the observed graph, between train and test set. All the generated subgraphs will contain the original nodes; only the edge set will be modified. We choose to retrieve the 10%  of the original edges set for the test graph while the remaining for training.

# COMMAND ----------

# u--> source node ID of the observed graph
# v --> destination node ID of the observed graph

u, v = fincen_graph.edges()

# generate unique ID for each edge
eids = np.arange(fincen_graph.number_of_edges())

# permute the element of "ieds" so that train and test are independent of their location (inside the original dataset)
eids = np.random.permutation(eids)

# take 10% of the whole edge set as test set
test_size = int(len(eids) * 0.1)

# take the remaining 90% as train set
train_size = fincen_graph.number_of_edges() - test_size

# source and destination of the edges in the test set
test_pos_u, test_pos_v = u[eids[:test_size]], v[eids[:test_size]]

# source and destination of the edges in the train set
train_pos_u, train_pos_v = u[eids[test_size:]], v[eids[test_size:]]

# using the chosen "positive" edges in the train set create the train positive graph (note all origianl nodes are present)
train_pos_fincen_graph = dgl.graph(
    (train_pos_u, train_pos_v), num_nodes=fincen_graph.number_of_nodes()
)

# using the chosen "positive" edges in the test set create the test positive graph (note all origianl nodes are present)
test_pos_fincen_graph = dgl.graph(
    (test_pos_u, test_pos_v), num_nodes=fincen_graph.number_of_nodes()
)

# the training graph
train_fincen_graph = dgl.remove_edges(fincen_graph, eids[:test_size])


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Train and test set for the negative example
# MAGIC
# MAGIC The negative example must be deduced from the positive example by generating a graph where previously un-connected pairs of nodes are now connected. To do so we first generate the adjacency matrix of the original matrix and the compute its complement.

# COMMAND ----------

# create a square SPARSE matrix of dimension "number of nodes". If btw 2 nodes there is an edge, the corresponding element is equal to 1, otherwise 0

# in order to create a square matrix I must create an artificial edge between the last node and itself that will be removed later

adj = sp.coo_matrix(
    (
        np.ones(
            len(u) + 1
        ),  # vector of 1´s; one for each edge existing + the last artificial edge btw the last node and itself
        (
            np.append(
                u.numpy(), fincen_graph.number_of_nodes() - 1
            ),  # row index to where to locate the 1´s of above. Note that the last node is appended
            np.append(v.numpy(), fincen_graph.number_of_nodes() - 1),
        ),
    )
).todense()  # col index to where to locate the 1´s of above. Note that the last node is appended

# remove the artificial edge
adj[fincen_graph.number_of_nodes() - 1, fincen_graph.number_of_nodes() - 1] -= 1.0

# create a matrix as "adj" but with values 1 for the elements corresponding to a pair of nodes NOT connected by an edge
adj_neg = 1 - adj - np.eye(fincen_graph.number_of_nodes())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In order to keep the positive and negative exmaple balacend we randomly select from _adj_neg_ a number of edges equal to the size of the edge set in the positive example, both in the train and test set.

# COMMAND ----------

# get source and destination node of all the "negative" edges
neg_u, neg_v = np.where(adj_neg != 0)

# randomly select "negative" edges to incldue in the analysis. The amount is equal to the "positive" edges found in the data
neg_eids = np.random.choice(len(neg_u), fincen_graph.number_of_edges())


# as for the "positive" edges, 10% of the "negative" edges will go in the test set and the remaining 90% will go in the train set
test_neg_u, test_neg_v = neg_u[neg_eids[:test_size]], neg_v[neg_eids[:test_size]]

train_neg_u, train_neg_v = neg_u[neg_eids[test_size:]], neg_v[neg_eids[test_size:]]

# using the chosen "negative" edges in the train set create the train negative graph (note all origianl nodes are present)
train_neg_fincen_graph = dgl.graph(
    (train_neg_u, train_neg_v), num_nodes=fincen_graph.number_of_nodes()
)


# using the chosen "negative" edges in the test set create the test negative graph (note all origianl nodes are present)
test_neg_fincen_graph = dgl.graph(
    (test_neg_u, test_neg_v), num_nodes=fincen_graph.number_of_nodes()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Storing the generated subgraphs
# MAGIC
# MAGIC The process performed so fare give us 5 subgraphs:
# MAGIC
# MAGIC - _train_fincen_graph
# MAGIC - _train_pos_fincen_graph
# MAGIC - train_neg_fincen_graph
# MAGIC - test_pos_fincen_graph
# MAGIC - test_neg_fincen_graph
# MAGIC
# MAGIC _train_fincen_graph_ will be used for training the model, _train_pos_fincen_graph_ and _train_neg_fincen_graph_ will serve the essential task of minimizing the loss while _test_pos_fincen_graph_ and _test_neg_fincen_graph_ will be used for performance evaluation. We now store all these graph to make them usable in future notebooks.

# COMMAND ----------

### Save the graphs

# save_graphs("/dbfs/mnt/public/clean/fincen-graph/train_fincen_graph", train_fincen_graph)

save_graphs(
    "/dbfs/mnt/public/clean/fincen-graph/train_pos_fincen_graph", train_pos_fincen_graph
)

save_graphs(
    "/dbfs/mnt/public/clean/fincen-graph/train_neg_fincen_graph", train_neg_fincen_graph
)

save_graphs(
    "/dbfs/mnt/public/clean/fincen-graph/test_pos_fincen_graph", test_pos_fincen_graph
)

save_graphs(
    "/dbfs/mnt/public/clean/fincen-graph/test_neg_fincen_graph", test_neg_fincen_graph
)
