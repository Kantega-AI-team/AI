# GraphSAGE - an inductive GNN

In many real-world problems, information can be naturally described using graphs, where the data structure is characterized by entities (i.e. molecules/users) and their relations (see _demo_graph_structure_ for a short introduction on graph objects). The versatility of the graph data structure allowed the ML community successfully delelop models for predictions and classifiction tasks such as:
- **Node classification**: given a graph with incomplete node labelling, predict the class of the remaining nodes
- **Link prediction**: given a graph with incomplete adjacency matrix, predict for each pair of nodes whether they are connected
- **Community detection** (a.k.a graph Clustering): given a graph, partition its nodes into clusters based on its edge structure
 
Often these methods use vector embeddings as (node and edges) feature inputs. Embeddings (or representations) are low-dimensional vectors computed by applying dimensionality reduction techinuques in order to filter the high-dimnesional information stored in the node features. However, the majority of these methods are so called transductive, requiring the observation of the complete graph structure. Instead, we now describe _GraphSAGE_ a general inductive framework which generates node embeddings for previously unseen data. Instead of training individual embeddings for each node, GraphSAGE learns a function that generates embeddings by sampling and aggregating features from a node’s local neighborhood. 

In this demo we briefly describe the learning strategy of the GraphSAGE model.

##Embedding generation

Following algorithm 1, the embeddings generation/update for each node follows two steps. Assuming we want to update the embedding of a generic target node, we have:

1. _aggregate_ neighbouring embedding vectors 
2.  _update_ the embedding of the target node.

At the aggregation step (line 4 in algorithm 1), the current embedding vectors appendend to the neighbouring nodes of the target are combined by mean of a function. A simple yet common aggregator function is the **mean** operator, where we simply take the element-wise mean of the neighbouring embeddings. A more flexible function is the **pooling** aggregator where each neighbor’s vector is independently fed through a fully-connected neural network. After this first tranformation, an element-wise max-pooling operation is applied to aggregate information across the neighbour set

![graphsage_image_2](/docs/images/graphsage_image_2.png =400x)

After obtaining an aggregated representation for the target node  based on its neighbours, we update the current embedding vector of the node using a combination of its previous embedding and the aggregated representation (line 5 in algorithm 1). The update function can be as simple as an averaging function, or as complex as a neural network.


![graphsage_image_1](/docs/images/graphsage_image_1.png)


The outer loop in algorithm 1 (line 2) describes an iterative learning process. For k=1 the embdedding vectors are learned using the "direct" neighbouring nodes; for $k=2$ the new embeddings are obtaind by combining info from the neighbours neighbours of the nodes and so on (see the image below).

![graphsage_image_3](/docs/images/graphsage_image_3.png =500x)

##Loss function

In the previous section we assumed that all the parameters where known  (the **W** matrices in line 5, algorithm 1 and the matrix **W** of the pool aggregator). In a fully unsupervised, in order to learn useful predictive embedding vector, a graph-based loss function is minimized via standard techinques such as stochastic gradient descent

![graphsage_image_4](/docs/images/graphsage_image_4.png =500x)

The blue portion of the loss function tries to enforce that if nodes u and v are close in the actual graph, then their node embeddings should be semantically similar. In the perfect scenario, we expect the inner product of $z_u$ and $z_v$ to be a large number. The sigmoid of this large number gets pushed towards 1 and the $log(1) = 0$. 
The pink portion of the loss function tries to enforce the opposite! That is, if nodes u and v are actually far away in the actual graph, we expect their node embeddings to be different/opposite. In the perfect scenario, we expect the inner product of $z_u$ and $z_v$ to be a large negative number. 

The product of two large negatives become a large positive number. The sigmoid of this large number gets pushed towards 1 and the $log(1) = 0$. Since there are potentially more nodes u that are far from our target node v in the graph, we sample only a few negative nodes u from the distribution of nodes far away from node v: $P_n(v)$. This ensures the loss function is balanced when training.

In cases where the embeddings are to be used for supervised tasks such as node classification or link prediction, the loss function will be replaced (e.g. cross-entropy loss).