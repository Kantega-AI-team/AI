# Databricks notebook source
# MAGIC %md
# MAGIC # Demo GNN - Link prediction
# MAGIC
# MAGIC In this demo we introduce briefly the _Graph Neural Network model_ and its applications. Specifically we focus on the task of _Link Prediction_, useful in many Machine Learning problems. The data preparation steps and the model training are carried using the *dgl* libray (https://www.dgl.ai)
# MAGIC
# MAGIC The following aspects will be covered in this demo/intro:
# MAGIC
# MAGIC - Basic definition of a Graph
# MAGIC - Motivation behind the usage of GNN
# MAGIC - Construction of a _graph dataset_: train and test
# MAGIC - Main idea behind Graphical Neural Network
# MAGIC - Basic definition of _message_, _reduce_ and _update_ function
# MAGIC - Positive and Negative examples
# MAGIC - The Score function
# MAGIC - Train a GNN
# MAGIC - Evaluation
# MAGIC
# MAGIC During the whole demo we will continuosly switch between text and code, trying to make the reading as smooth as possible. Sometimes, when necessary, comments will be added directly to the code; read them, they might turn out useful.
# MAGIC
# MAGIC Note: If you are a Python expert: this demo has been made in Python by somebody terrible at it, have mercy. If you are a Python beginner: welcome to the club, I warn you there will be a lot of scary things such as f.e. **torch** vectors and **super** declarations. Take it easy and try to look at the whole picture before going into details.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Definition of a Graph
# MAGIC
# MAGIC A graph 𝐺=(𝑉,𝐸) is a structure used to represent entities and their relations. It consists of two sets – the set of nodes 𝑉 (also called vertices) and the set of edges 𝐸 (also called arcs). An edge (𝑢,𝑣) ∈ 𝐸  connecting a pair of nodes 𝑢 and 𝑣 indicates that there is a relation between them. The relation can either be undirected, e.g., capturing symmetric relations between nodes, or directed, capturing asymmetric relations. For example, if a graph is used to model the friendships relations of people in a social network, then the edges will be undirected as friendship is mutual; however, if the graph is used to model how people follow each other on Twitter, then the edges are directed. Depending on the edges’ directionality, a graph can be directed or undirected.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nodes and edges of a graph
# MAGIC
# MAGIC In Python a directed graph dataset can be built in few easy steps:
# MAGIC - create _src_ (referred as 𝑢 above)  and _dst_ (referred as 𝑣 above) tensor vectors where the source and destination node of each edge is stored
# MAGIC - use the function _graph_ from the dgl library to assemble the graph

# COMMAND ----------

import dgl
import networkx as nx
import torch as th

# create two tensor vectors: "src" and "dst"

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

# create the graph
graph_1 = dgl.graph((src, dst))

# to view the edges of the graph run:
# graph_1.edges()

# to view the nodes of the graph run:
# graph_1.nodes()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Plot a graph
# MAGIC For plotting the graph we need to convert it to a _networkx_ graph using the homonym library

# COMMAND ----------

import networkx as nx

# for plotting the graph we need to convert the "dgl" graph in a "networkx" graph

nx_graph_1 = graph_1.to_networkx()

# We need to use a layout. I have no idea of the possbile choices so I copy & paste (Kamada-Kawaii layout usually looks pretty for arbitrary graphs)

pos = nx.kamada_kawai_layout(nx_graph_1)

nx.draw(nx_graph_1, pos, with_labels=True)


# COMMAND ----------

# MAGIC %md
# MAGIC Two useful things are worthed mentioning in relation the graph building:
# MAGIC - going from directed to undirected graph by creating edges for both directions
# MAGIC - create a graph with stand-alone nodes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### From directed to undirected Graph
# MAGIC
# MAGIC Let's start by transforming the _graph_1_, directed graph created above, in an undirected graph; we need only one command

# COMMAND ----------

# make graph_1 undirected
graph_1_un = dgl.to_bidirected(graph_1)

graph_1_un.edges()  # more edges than before!

graph_1_un.nodes()  #  same nodes than before

# let´s plot it

nx_graph_1_un = graph_1_un.to_networkx()

# We need to use a layout. I have no idea of the possbile choices so I copy & paste (Kamada-Kawaii layout usually looks pretty for arbitrary graphs)
pos = nx.kamada_kawai_layout(nx_graph_1)

# drae the graph
nx.draw(nx_graph_1_un, pos, with_labels=True)


# COMMAND ----------

# MAGIC %md
# MAGIC As you can see from the above plot, each edge is bi-directional. Note also that the edge between nodes 1 and 4 was already bi-directional and has not been modified.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Specify number of nodes
# MAGIC
# MAGIC Now let´s go back to our directed graph, _graph_1_. This time we create it specifying a total number of nodes to be included in the graph equal to 7.

# COMMAND ----------

import dgl
import networkx as nx
import torch as th

# create two tensor vectors: "src" and "dst"

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

graph_1_plus = dgl.graph((src, dst), num_nodes=7)

# to view the edges of the graph run:
# graph_1_plus.edges()

# to view the nodes of the graph run:
# graph_1_plus.nodes()

# COMMAND ----------

import networkx as nx

# for plotting the graph we need to convert the "dgl" graph in a "networkx" graph

nx_graph_1_plus = graph_1_plus.to_networkx()

# We need to use a layout. I have no idea of the possbile choices so I copy & paste (Kamada-Kawaii layout usually looks pretty for arbitrary graphs)

pos = nx.kamada_kawai_layout(nx_graph_1_plus)

# draw the graph
nx.draw(nx_graph_1_plus, pos, with_labels=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add edges after the creation of a graph
# MAGIC You see now that nodes 5 and 6 are included in the graph but have no edges connected. It is also possible to add edges after the creation of the graph dataset using the function _add_edges_. Let'see how

# COMMAND ----------

# add 2 edges to graph_1: 5-->4 and 6-->4

graph_1.add_edges(th.tensor([5, 6]), th.tensor([4, 4]))

import networkx as nx

# for plotting the graph we need to convert the "dgl" graph in a "networkx" graph

nx_graph_1 = graph_1.to_networkx()

# We need to use a layout. I have no idea of the possbile choices so I copy & paste (Kamada-Kawaii layout usually looks pretty for arbitrary graphs)

pos = nx.kamada_kawai_layout(nx_graph_1)

# draw the graph
nx.draw(nx_graph_1, pos, with_labels=True)


# COMMAND ----------

# import required libraries

import itertools
import os

import dgl
import numpy as np
import pandas as pd
import scipy.sparse as sp
import torch
import torch as th
import torch.nn as nn
import torch.nn.functional as F
from dgl.data import DGLDataset

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Node and edges Features
# MAGIC Now, we have an idea of the structure of a graph in terms of nodes and edges. Both nodes an edges can have attached features, describing useful characteristics of the entitiies unnder study (the nodes) and their relations (the edges). For example, when considering social media data it is  natural to assume age and citizenships as features attached to each node (the users) and the presence of friendship and/or the number of years since two users are friends as features attached to the edges. We now briefly describe how to attach node and edge features to a graph.
# MAGIC
# MAGIC Starting for _graph_1_ we attacch a 1-dimensinal feature to the nodes and a 2-dimensional feature to the edges

# COMMAND ----------


# create two tensor vectors: "src" and "dst"

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

# create the graph
graph_1 = dgl.graph((src, dst))

# append the 1-dimensional node feature( a number for each node) and call it "h"
graph_1.ndata["h"] = th.tensor([[5.0], [10.0], [15.0], [20.0], [25.0]])

# append the 2-dimensional edge feature( a 2-dim vector for each edge) and call it "e"
graph_1.edata["e"] = th.tensor(
    [
        [3.0, 5.0],
        [4.0, 6.0],
        [40.0, 60.0],
        [13.0, 15.0],
        [24.0, 36.0],
        [11.0, 22.0],
        [2.0, 7.0],
    ]
)

# check the structure of the graph and the features
graph_1


# COMMAND ----------

# MAGIC %md
# MAGIC In order to append features we must use torch tensor, which are basically vectors. The node features are stored inside the graph under the _ndata_ method while the edge features are stored under _edata_.  All the the features must be numeric.
