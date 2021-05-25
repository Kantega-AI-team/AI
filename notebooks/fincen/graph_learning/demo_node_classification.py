# Databricks notebook source
# import required libraries
import itertools
import os
import random

import dgl
import dgl.function as fn
import numpy as np
import pandas as pd
import scipy.sparse as sp
import torch as th
import torch.nn as nn
import torch.nn.functional as F
from dgl.data import DGLDataset
from dgl.data.utils import load_graphs
from dgl.nn import SAGEConv
from matplotlib import pyplot
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)

# COMMAND ----------

# set seed for reproducibility
th.manual_seed(4)

# COMMAND ----------

# load fincen graph
fincen_graph = load_graphs("/dbfs/mnt/public/clean/fincen-graph/fincen_dgl")[0][0]
fincen_graph

# COMMAND ----------

# load train_fincen_graph - used to train the GNN
train_fincen_graph = load_graphs(
    "/dbfs/mnt/public/clean/fincen-graph/train_fincen_graph"
)[0][0]

# COMMAND ----------

# Overview of node features
train_fincen_graph.ndata

# COMMAND ----------

# Overview of edge features
train_fincen_graph.edata

# COMMAND ----------

# Plotting the number of transactions between the banks
edge_feat1 = fincen_graph.edata["feat_edges"][:, 1]

pyplot.hist(edge_feat1)


# COMMAND ----------

# The standardize feature for bank is greater than 2

illicit_banks = fincen_graph.edges()[0][edge_feat1 > 2].unique()

len(illicit_banks)  # 50 nodes labelled as (illicit) (2.5%)

# COMMAND ----------

# Create tensor hosting the ground truth for each node
outcome = th.zeros(len(fincen_graph.nodes()))
outcome[illicit_banks] = 1

sum(outcome)

# COMMAND ----------

# Create index for the nodes, and split those indices into train and test sets, 80%, 20%

index_vec = np.arange(start=0, stop=len(fincen_graph.nodes()), step=1)
train_nodes, test_nodes = th.utils.data.random_split(index_vec, [1518, 400])

# COMMAND ----------

#  Make train_mask and test_mask with value = 1 for train nodes and test nodes respectively

train_mask = th.zeros(len(fincen_graph.nodes()), dtype=th.bool).reshape(
    len(fincen_graph.nodes()), 1
)
train_mask[train_nodes] = 1

test_mask = th.zeros(len(fincen_graph.nodes()), dtype=th.bool).reshape(
    len(fincen_graph.nodes()), 1
)
test_mask[test_nodes] = 1

# sum(test_mask)

test_mask.shape

# COMMAND ----------

# Concatinate to fincen_graph the train_mask and test_mask

fincen_graph.ndata["train_mask"] = train_mask
fincen_graph.ndata["test_mask"] = test_mask

# Also add ground truth to label column
fincen_graph.ndata["label"] = outcome

# COMMAND ----------

"""
Customized SAGEconv layer: it uses also the edge features
Build a two-layer GraphSAGE model
"""


class GraphSAGE(nn.Module):
    """
    Initialize values
    @param: in_feats: dimension of the initial embedding
    @param: h_feats: dimension of the output embedding
    """

    def __init__(self, in_feats, h_feats):
        super(GraphSAGE, self).__init__()
        self.conv1 = SAGEConv(in_feats, h_feats, "mean")  # First GraphSAGE layer
        self.conv2 = SAGEConv(h_feats, h_feats, "mean")  # Second GraphSAGE layer

    """
    Forward pass
    @param: g: the graph
    @param: feature_nodes: nodes of the graph
    @param: feature_edges: edges of the graph
    """

    def forward(self, g, feature_nodes, feature_edges):
        g.ndata["h_n"] = feature_nodes  # Store the node features in the  graph as "h"
        g.edata["e"] = feature_edges  # Store the edge features in the graph as "e"

        g.update_all(
            fn.copy_e("e", "m_e"), fn.mean("m_e", "h_e")
        )  # Take all the features attached to all the in-coming edges of a node and perform the mean; call the output vector "h_e"

        g.ndata["h"] = th.cat(
            (g.ndata.pop("h_n"), g.ndata.pop("h_e")), 1
        )  # Create a new embedding for each node by concatenating the vectors "h_e" and "h_n"; call the embedding "h"
        h_n = g.ndata["h"]  # Store the new embedding matric as "h_n"
        h = self.conv1(g, h_n)  # Perform the first convolutional layer
        h = F.relu(h)  # Apply non-linearity
        h = self.conv2(g, h)  # Perform the second convolutional layer
        return h


# COMMAND ----------

# Register the Neural Network with the "outer" input
model = GraphSAGE(
    train_fincen_graph.ndata["feat_nodes"].shape[1]
    + train_fincen_graph.edata["feat_edges"].shape[1],
    2,
)

# Train the model
# Forward
optimizer = th.optim.Adam(model.parameters(), lr=0.01)
best_test_acc = 0

labels = fincen_graph.ndata["label"]
train_mask = fincen_graph.ndata["train_mask"]
test_mask = fincen_graph.ndata["test_mask"]
epochs = 100

for e in range(epochs):
    logits = model(
        fincen_graph,
        fincen_graph.ndata["feat_nodes"].float(),
        fincen_graph.edata["feat_edges"].float(),
    )

    # Compute prediction
    pred = logits.argmax(1)
    ex_score = logits[:, 1]
    train_mask_flattened = train_mask.flatten()
    test_mask_flattened = test_mask.flatten()

    # Compute loss

    # Note that you should only compute the losses of the nodes in the training set
    loss = F.binary_cross_entropy_with_logits(
        ex_score[train_mask_flattened], labels[train_mask_flattened]
    )

    # Compute accuracy on training/test
    train_acc = (
        (pred[train_mask_flattened] == labels[train_mask_flattened]).float().mean()
    )
    test_acc = (pred[test_mask_flattened] == labels[test_mask_flattened]).float().mean()

    # Save the best test accuracy
    if best_test_acc < test_acc:
        best_test_acc = test_acc

    # Backward
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    if e % 5 == 0:
        print(
            "In epoch {}, loss: {:.3f}, test acc: {:.3f} (best {:.3f})".format(
                e, loss, test_acc, best_test_acc
            )
        )


# COMMAND ----------

# Test phase: Select random indexes from test nodes and get the labels for the nodes.
indices = random.choices(test_nodes.indices, k=20)
pyplot.hist(pred[indices])

# COMMAND ----------

# Actual answers
pyplot.hist(labels[indices])
