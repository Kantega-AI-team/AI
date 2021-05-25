# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Graph structure
# MAGIC
# MAGIC In this demo we introduce briefly the graph object. The goal is to provide the fundamental notions and computational skills to create a graph and its features. Knowledge about graphs is preparatory for the study Graph Neural Network model. During the demo we will continuously switch between text and  Python code, trying to make the reading as smooth as possible. Sometimes, when necessary, comments will be added directly to the code; read them, they might turn out to be useful. Most of the commands for perfotming graph manipulation come from the Python *dgl* libray (https://www.dgl.ai)
# MAGIC
# MAGIC The following aspects will be covered in this demo:
# MAGIC
# MAGIC - Basic definition of a Graph
# MAGIC - Nodes and edges of a graph
# MAGIC - Graph visualization
# MAGIC - From directed to undirected Graph
# MAGIC - Specify number of nodes
# MAGIC - Add edges after the creation of a graph
# MAGIC - Node and edge features
# MAGIC - The FINCen dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Definition of a Graph
# MAGIC
# MAGIC A graph 𝐺=(𝑉,𝐸) is a structure used to represent entities and their relations. It consists of two sets – the set of nodes 𝑉 (also called vertices) and the set of edges 𝐸 (also called arcs). An edge (𝑢,𝑣) ∈ 𝐸  connecting a pair of nodes 𝑢 and 𝑣 indicates that there is a relation between them. The edges can either be undirected, e.g., capturing symmetric relations between nodes, or directed, capturing asymmetric relations. For example, if a graph is used to model the friendship status of people in a social network, then the edges will be undirected as friendship is mutual; however, if the graph is used to model how people follow each other on Twitter, then the edges are directed. Depending on the edges’ directionality, a graph can be directed or undirected.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nodes and edges of a graph
# MAGIC
# MAGIC In Python a directed graph dataset can be built in few easy steps:
# MAGIC - create _src_ (referred as 𝑢 above)  and _dst_ (referred as 𝑣 above) tensor vectors where the source and destination node of each edge is stored
# MAGIC - use the function _graph_ from the dgl library to assemble the graph

# COMMAND ----------

# import required libraries
import dgl
import networkx as nx
import pandas as pd
import torch
import torch as th
from dgl.data.utils import save_graphs

# create two tensor vectors: "src" and "dst"

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

# create the graph
dgl_graph = dgl.graph((src, dst))

# to view the edges of the graph run:
# dgl_graph.edges()

# there are 7 edges in total
# len(dgl_graph.edges()[1])

# to view the nodes of the graph run:
# dgl_graph.nodes()

# there are 5 nodes in total
len(dgl_graph.nodes())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Graph visualization
# MAGIC For plotting the graph we need to convert it to a _networkx_ graph using the _networkx_ library

# COMMAND ----------

# for plotting the graph we need to convert the "dgl" graph in a "networkx" graph and choose a layout. In order to easily plot graph in later chunks we create a simple function for drawing the graph


def plot_the_graph(dgl_graph):

    nx_dgl_graph = dgl_graph.to_networkx()  # convert from dgl to networkx
    pos = nx.kamada_kawai_layout(nx_dgl_graph)  # choose the layout
    nx.draw(nx_dgl_graph, pos, with_labels=True)  # draw the graph


# plot dgl_graph
plot_the_graph(dgl_graph)


# COMMAND ----------

# MAGIC %md
# MAGIC Two useful things are worthed mentioning in relation the graph building:
# MAGIC - going from directed to undirected graph by creating edges for both directions
# MAGIC - create a graph with stand-alone nodes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## From directed to undirected Graph
# MAGIC
# MAGIC Let's start by transforming the _graph_1_, directed graph created above, in an undirected graph; we need only one command

# COMMAND ----------

# make dgl_graph undirected
dgl_graph_un = dgl.to_bidirected(dgl_graph)

len(dgl_graph_un.edges()[1])

# dgl_graph_un.nodes()  #same nodes than before

# let´s plot it

# plot_the_graph(dgl_graph_un)


# COMMAND ----------

# MAGIC %md
# MAGIC As you can see from the above plot, each edge is bi-directional. Note also that the edge between nodes 1 and 4 was already bi-directional and has not been modified.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specify number of nodes
# MAGIC
# MAGIC Now let´s go back to our directed graph, _graph_1_. This time we create it specifying a total number of nodes to be included in the graph equal to 7.

# COMMAND ----------

# create two tensor vectors:

# "src" and "dst"  define relations including nodes (0,1,2,3,4)

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

# with num_nodes = 7 we customize the number nodes in the graph. The nodes 5 and 6 will not be the "src" or "dst" of any edges (as specified in row 5 and 6)
dgl_graph_extended = dgl.graph((src, dst), num_nodes=7)

# to view the edges of the graph run:
# dgl_graph_extended.edges()

# to view the nodes of the graph run:
# dgl_graph_extended.nodes()

# let´s plot the graph

plot_the_graph(dgl_graph_extended)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Add edges after the creation of a graph
# MAGIC You see now that nodes 5 and 6 are included in the graph but have no edges connected. It is also possible to add edges after the creation of the graph dataset using the function _add_edges_. Let'see how

# COMMAND ----------

# add 2 edges to graph_1: 5-->0 and 6-->4

dgl_graph_extended.add_edges(th.tensor([5, 6]), th.tensor([0, 4]))

plot_the_graph(dgl_graph_extended)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Node and edge Features
# MAGIC Now, we have an idea of the structure of a graph in terms of nodes and edges. Both nodes an edges can have attached features, describing useful characteristics of the entitiies unnder study (the nodes) and their relations (the edges). For example, when considering social media data it is  natural to assume age and citizenships as features attached to each node (the users) and the presence of friendship and/or the number of years since two users are friends as features attached to the edges. We now briefly describe how to attach node and edge features to a graph.
# MAGIC
# MAGIC Starting with _dgl_graph_ we attacch a 1-dimensinal feature to the nodes and a 2-dimensional feature to the edges. In order to append features we must use torch tensors, which are basically vectors. The node features are stored inside the graph under the _ndata_ method while the edge features are stored under _edata_. All the the features must be numeric.

# COMMAND ----------

# create two tensor vectors: "src" and "dst"

src = th.tensor([0, 2, 3, 2, 3, 1, 4])
dst = th.tensor([1, 1, 2, 3, 0, 4, 1])

# create the graph
dgl_graph = dgl.graph((src, dst))

# append the 1-dimensional node feature( a number for each node) and call it "h"
dgl_graph.ndata["h"] = th.tensor([[5.0], [10.0], [15.0], [20.0], [25.0]])

# append the 2-dimensional edge feature( a 2-dim vector for each edge) and call it "e"
dgl_graph.edata["e"] = th.tensor(
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
dgl_graph


# COMMAND ----------

# MAGIC %md
# MAGIC ## The FINCen dataset
# MAGIC
# MAGIC We now  create a graph object from a real dataset. The FINCen dataset contains monetary transactions from all over the world. The entities are the banks while the edges imply money transfers. As node feature we have the country where the bank was located. There are 2 features associated with the edge:
# MAGIC 1. Amount of the transaction
# MAGIC 2. Number of sub-transactions

# COMMAND ----------

# dataset containing the nodes of the graph ("bank", "country", "id" )
nodes_original = spark.read.load("/mnt/public/clean/fincen-graph/vertices").toPandas()

# clean dataset containing the edges of the graph (src & dst columns) and the related features ("amount_transactions", "number_transactions")
edges_original = spark.read.load("/mnt/public/clean/fincen-graph/edges").toPandas()
# the dataset is directed; make it undirected

edges_data2 = edges_original.loc[
    :, ["dst", "src", "amount_transactions", "number_transactions"]
]

# switch column names sr_ <--> dst
edges_data2.columns = ["src", "dst", "amount_transactions", "number_transactions"]

# append df2 in df
edges_data = edges_original.append(edges_data2, ignore_index=True)

# add a column counting how many time each row is observed in the dataset
edges_data["count"] = edges_data.groupby(["src", "dst"])["src"].transform("size")

# select only row observed once (count==1)
edge1 = edges_data.loc[edges_data["count"] == 1]

# select only rows observed twice (count == 2) -->
edge2 = edges_data.loc[edges_data["count"] == 2]

edge2 = edge2.groupby(["src", "dst"]).sum().reset_index()

# create final edge dataset by appending edge1 and edge2
edges_data = edge1.append(edge2).drop("count", axis=1).reset_index(drop=True)


# CREATE THE GRAPH


# vector with starting point of each edge
edges_src = torch.from_numpy(edges_data["src"].to_numpy())

# vector with ending point of each edge
edges_dst = torch.from_numpy(edges_data["dst"].to_numpy())


# create  the graph
graph_fincen = dgl.graph((edges_src, edges_dst), num_nodes=nodes_original.shape[0])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature manipulation
# MAGIC Prior to be included in the graph, the node feature _country_ is transformed as dummy. Each country is coded as a unique vector of lengh _ # of unique countries observed in the data_  with all elements equal to zero but in one (where the value is actually 1). For the two edge features we apply the standardization (x-x.mean)/x.sd

# COMMAND ----------

# create node features: dummies encoding for the country
node_features = torch.from_numpy(
    pd.get_dummies(nodes_original["country"], columns=["country"]).to_numpy()
)

# display the matrix of the node features
# node_features.head()

# create edge features
edge_features = torch.from_numpy(
    edges_original[["amount_transactions", "number_transactions"]].to_numpy()
)

# standardize the edge features

edges_data[["amount_transactions", "number_transactions"]] = (
    edges_data[["amount_transactions", "number_transactions"]]
    - edges_data[["amount_transactions", "number_transactions"]].mean()
) / edges_data[["amount_transactions", "number_transactions"]].std()

edges_data


# COMMAND ----------

### GRAPH GENERATION

# vector with starting point of each edge
edges_src = torch.from_numpy(edges_data["src"].to_numpy())

# vector with ending point of each edge
edges_dst = torch.from_numpy(edges_data["dst"].to_numpy())


# create  the graph
graph_fincen = dgl.graph((edges_src, edges_dst), num_nodes=nodes_original.shape[0])


# create edge features
edge_features = torch.from_numpy(
    edges_data[["amount_transactions", "number_transactions"]].to_numpy()
)

# attach the node feature to the graph dataset name the matrix "feat_nodes"
graph_fincen.ndata["feat_nodes"] = node_features

# attach the edge features to the graph dataset - name the matrix "feat_edges"
graph_fincen.edata["feat_edges"] = edge_features

graph_fincen


# COMMAND ----------

# MAGIC %md
# MAGIC  ### Save the graph
# MAGIC
# MAGIC  The final graph _graph_fincen_ is then stored for later use

# COMMAND ----------

save_graphs("/dbfs/mnt/public/clean/fincen-graph/fincen_dgl", graph_fincen)
