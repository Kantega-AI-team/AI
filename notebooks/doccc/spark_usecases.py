# Databricks notebook source

import mlflow.sklearn
import mlflow.spark
import numpy as np
from joblibspark import register_spark
from pyspark.ml.classification import RandomForestClassifier as RandomForestClfSpark
from pyspark.ml.feature import VectorAssembler
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier as RandomForestClfSklearn
from sklearn.metrics import f1_score
from sklearn.model_selection import GridSearchCV, cross_val_score
from sklearn.utils import parallel_backend

# COMMAND ----------

# MAGIC %md ## Spark for machine learning
# MAGIC
# MAGIC There are four main use cases for Spark with Machine Learning
# MAGIC - Data preprocessing / ML Pipelines
# MAGIC - Cross validation
# MAGIC - Hyperparameter tuning
# MAGIC - Distributed learning
# MAGIC
# MAGIC The first has already been implicitly employed with the data flow used for the doccc data. We will now take a look at cross validation and hyperparameter tuning

# COMMAND ----------

# MAGIC %md ####Get data

# COMMAND ----------

# MAGIC %run ./gold_asif_stream

# COMMAND ----------

dbutils.fs.rm("/mnt/public/gold/doccc/mock_stream_spark_usecases", True)

# COMMAND ----------

silver_path = "/mnt/public/silver/doccc/doccc"
description_path = "/mnt/public/silver/doccc/field_descriptions"
gold_path = "/mnt/public/gold/doccc/mock_stream_spark_usecases"
validation_path = "/mnt/public/gold/doccc/mock_stream_validation_spark_usecases"
silver_data = spark.read.format("delta").load(silver_path)

# COMMAND ----------

GMS = GoldMockStream(
    full_data=silver_data,
    first_bulk_size=5000,
    validation_size=25000,
    gold_path=gold_path,
    validation_path=validation_path,
    num_steps=1,
)


df = spark.read.format("delta").load(gold_path)
pdf = df.toPandas()
X = pdf.drop(["ID", "Y"], axis=1)
y = pdf["Y"]

# COMMAND ----------

# MAGIC %md ### Spark for cross validation
# MAGIC
# MAGIC Although arguably one of the more robust model selection strategies, cross validation has one main drawback: Compute. For 10 fold CV, we must train a model with 90% of the data 10 times, instead of with a simple train/test/validation split where we hopefully do a fairly representative draw.
# MAGIC
# MAGIC Spark is useful for this task, since we may distribute the load on several workers

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# Without spark
# Without spark
@timing
def timed_cross_val_score(clf, X, y, cv, n_jobs):
    with mlflow.start_run() as run:
        mlflow.log_metric(
            "mean_cv_score",
            np.mean(cross_val_score(clf, X, y, cv=cv, n_jobs=n_jobs, scoring="f1")),
        )


timed_cross_val_score(RandomForestClassifier(), X, y, cv=5, n_jobs=None)

# COMMAND ----------

# With spark - scikit-learn already implements parallell backend appicable to Spark
# Applied by setting n_jobs

@timing
def timed_cross_val_score(clf, X, y, cv, n_jobs):
    with mlflow.start_run() as run:
        mlflow.log_metric(
            "mean_cv_score",
            np.mean(cross_val_score(clf, X, y, cv=cv, n_jobs=n_jobs, scoring="f1")),
        )


timed_cross_val_score(RandomForestClassifier(), X, y, cv=5, n_jobs=16)

# COMMAND ----------

# MAGIC %md ### Spark for hyperparameter tuning
# MAGIC
# MAGIC The line of arguments is almost excactly the same here. We leverage paralell runs using spark.

# COMMAND ----------

param_grid = {
    "max_depth": [3, None],
    "max_features": [1, 3, 10],
    "min_samples_split": [0.2, 0.5],
    "min_samples_leaf": [0.1, 0.4],
    "bootstrap": [True],
    "criterion": ["gini"],
    "n_estimators": [10, 50, 80],
}
# COMMAND ----------


@timing
def timed_rf_tuning(estimator, parameters, cv):
    return GridSearchCV(
        estimator=estimator, param_grid=parameters, cv=cv, verbose=2, scoring="f1"
    ).fit(X, y)


# COMMAND ----------

# Not utilizing spark
with mlflow.start_run() as run:
    timed_rf_tuning(estimator=RandomForestClassifier(), parameters=param_grid, cv=8)

# COMMAND ----------

# Using joblibspark to distribute tasks on cluster
register_spark()
with mlflow.start_run() as run:
    with parallel_backend("spark", n_jobs=8):
        timed_rf_tuning(estimator=RandomForestClassifier(), parameters=param_grid, cv=8)
