# Kantega AI

This project demonstrates multiple applied AI examples built on top of a solid and highly scalable dataplatform.


## Dataplatform
<TODO: Short description and motivation>
<TODO: One md-file or notebook uniting the set of notebooks related to delta lake data flow>

Environment: Python, Delta Lake

## Graph Learning
>"Graphs are networks of dots and lines." 
*- Richard J. Trudeau*

![Graph](/docs/images/graph.png =200x)

One of the areas where machine learning has done most progress over the last year is within graph learning. 
That is, machine learning applied on graph structured data. In the section below, we will show examples of techniques applied to identify the most influentian nodes and sub-communities in a graph, and how we can predict unseen properties and relationships.



### Community Detection
Community Detection is one of the fundamental problems in network analysis, where the goal is to find groups of nodes that are thightly related and more similar to each other than to the other nodes.
There are several approached for detecting communities within a larger graph structure. While some techniques traverse graph relations, others do more traditional clustering in an embedding space.

The notebook example below use Apache Spark to represent the FinCen Files dataset as a GraphX graph. Then, the Pagerank algorithm is used to score the significance and importance of each node (bank), before a label propagation algorithm is used to detect communities of highly connected banks.

![Community Detection](/docs/images/community_detection.png =250x)

[**Notebook - Community Detection**](/notebooks/graph_models/community_detection.py)
* Environment: PySpark
* Dataset: [FinCEN Files](https://www.icij.org/investigations/fincen-files/explore-the-fincen-files-data/)

### Graph Neural Networks
<TODO: Short description and motivation>

#### Node classification / regression
<TODO: Short description and motivation>

tekst

![Node Classification](/docs/images/node_classification.png =250x)

<TODO: Notebook>
* Environment: Python, DGL
* Dataset: ?

#### Link classification / regression
<TODO: Short description and motivation>

tekst

![Node Classification](/docs/images/link_prediction.png =250x)

<TODO: Notebook>
Environment: Python, DGL
Dataset: ?

## Visualization
<TODO: Short description and motivation>

### Shiny Apps
Shiny is an R package that makes it easy to build interactive web apps straight from R. You can host standalone apps on a webpage or embed them in R Markdown documents or build dashboards.

In our notebook example, we use shiny apps to create an interactive visualization where users can explore historical earth quakes.

![Shiny Apps](/docs/images/rshiny_logo.png =250x)
![Shiny Screenshots](/docs/images/shiny_example.png =250x)

[**Notebook - Shiny Apps**](/notebooks/shiny/shiny_example.py)
* Environment: R
* Dataset: Earth quake data

## Machine Learning Methodologies

### Active Learning
Active learning is a special case of machine learning in which a learning algorithm can interactively query a user (or some other information source) to label new data points with the desired outputs. 

Our notebook example shows how Active Learning allows you to train a high quality classifier model with a reduction of labeled training samples of 90% (in comparison to random sampling). 

Active learning is very suitable for environments where machines and human experts work together as a team. The machines will improve their capabilities quickly and, over time, also adapt and detect new data patterns. 
While they take care of the simpler cases, humans can spend their capacity on the challenging border cases where expert knowledge and wider experience are required. 

[**Notebook - Active Learning**](/notebooks/elliptic/active_learning_elliptic_shap.py)
* Environment: Python
* Dataset: [Elliptic Dataset](https://www.kaggle.com/ellipticco/elliptic-data-set)

