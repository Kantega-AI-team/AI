# ML Architecture

The AML AI project runs consist of the following components

![ml architecture](/docs/images/ml_architecture.png)

## Raw storage

Raw datasets are stored in Azure Data Lake, in the container with name `raw`. Formats are as is, for instance `.csv`.

## Pre processing

Databricks runs notebooks to clean and preprocess datasets. The datasets should be ready for usage in machine learning experiments and projects, and stored as [delta parquet](https://delta.io/). The curated datasets should be stored in Azure Data Lake, in the container with name `clean`.

## ML Ingest and experiments

The datasets stored as Delta Parquet are efficient and simple to read in Databricks, using either the `R` or `Python` API. The Databricks workspace comes preinstalled with all major machine learning libraries, and the machine learning lifecycle platform [mlflow](https://mlflow.org/).
