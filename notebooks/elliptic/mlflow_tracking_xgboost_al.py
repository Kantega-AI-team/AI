# Databricks notebook source
# MAGIC %md #MLFlow tracking concepts
# MAGIC
# MAGIC For this notebook, we will use the elliptic dataset and active learning to showcase some features in the [Mlflow Tracking component](https://www.mlflow.org/docs/latest/tracking.html)

# COMMAND ----------

import mlflow
import mlflow.xgboost
import numpy as np
import xgboost as xgb
from mlflow.models.signature import infer_signature
from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from sklearn.metrics import f1_score, recall_score

# COMMAND ----------

# MAGIC %md ## Get data

# COMMAND ----------

# Read data and display a sample
df = spark.read.format("delta").load("/mnt/public/clean/elliptic")
display(df.limit(10))


# COMMAND ----------

# MAGIC %md ## Minor cleanup steps

# COMMAND ----------

# Some minor cleaning steps
df = df.drop("txId", "simple_kr", "real_kr").withColumn(
    "class", col("class").cast(IntegerType())
)
display(df.limit(10))

# COMMAND ----------

# MAGIC %md ##Train/test split

# COMMAND ----------

# Train/test split as in article arXiv:2005.14635
train = df.filter(col("time") <= 34).drop("time").toPandas()
test = df.filter(col("time") > 34).drop("time").toPandas()

# Pandas x/y split
X_train, y_train = train.drop(columns=["class"]), train[["class"]]
X_test, y_test = test.drop(columns=["class"]), test[["class"]]

# COMMAND ----------

# MAGIC %md ##XGBoost Experiment

# COMMAND ----------

with mlflow.start_run(run_name="Basic XGBoost Experiment") as run:
    max_depth = 6

    # Create xgboost classifier with default params
    xgb_classifier = xgb.XGBClassifier(
        max_depth=max_depth, verbosity=0
    )  # same as default, to showcase mlflow log
    xgb_classifier.fit(X_train, y_train)
    mlflow.log_param("max_depth", max_depth)

    # Fit to training data
    xgb_classifier.fit(X_train, y_train)

    # Make predictions
    predictions = xgb_classifier.predict(X_test)

    # Calculate f1 score
    f1score_xgb = f1_score(y_test, predictions)
    print("F1-score for XGB: {acc:0.4f}".format(acc=f1score_xgb))
    mlflow.log_metric("f1", f1score_xgb)

    # Calculate recall
    recall_xgb = recall_score(y_test, predictions)
    print("Recall-score for XGB: {acc:0.4f}".format(acc=recall_xgb))
    mlflow.log_metric("recall", recall_xgb)

    # Run information
    runID = run.info.run_uuid
    experimentID = run.info.experiment_id
    signature = infer_signature(train, predictions)

    # Add a tag
    mlflow.set_tag("dataset", "elliptic")

    # Save model
    mlflow.xgboost.log_model(
        xgb_model=xgb_classifier, artifact_path="xgboost-model", signature=signature
    )

    print(
        "Inside MLflow Run with run_id {} and experiment_id {}".format(
            runID, experimentID
        )
    )
    mlflow.end_run()

# COMMAND ----------

# MAGIC %md ##Active learning Experiment, using nested runs

# COMMAND ----------

# Create some renamed objects
X_raw, y_raw, X_test_al, y_test_al = (
    X_train.to_numpy(),
    y_train.values.ravel().astype(int),
    X_test.to_numpy(),
    y_test.values.ravel().astype(int),
)

# Draw sample using only first observations
n_labeled_examples = X_raw.shape[0]

# COMMAND ----------

# Set initial sample size
sample_size = 1
N_QUERIES = 1500
with mlflow.start_run(run_name="Active Learning") as run:

    # Draw sample
    training_indices = np.random.randint(
        low=0, high=n_labeled_examples + 1, size=sample_size
    )

    # Put observations in training set for active larning
    X_train_al = X_raw[training_indices]
    y_train_al = y_raw[training_indices]

    # Put remaining observations in a pool
    X_pool = np.delete(X_raw, training_indices, axis=0)
    y_pool = np.delete(y_raw, training_indices, axis=0)

    mlflow.log_param("sample_size", 1)
    xg_boost = xgb.XGBClassifier(verbosity=0)
    learner = ActiveLearner(
        estimator=xg_boost,
        query_strategy=uncertainty_sampling,
        X_training=X_train_al,
        y_training=y_train_al,
    )

    # Predict for test data
    predictions_xgb = learner.predict(X_test_al)

    # Add a tag
    mlflow.set_tag("dataset", "elliptic")

    # Log f1-score
    unqueried_f1_xgb = f1_score(y_test_al, predictions_xgb)
    print("Initial F1-score for XGB: {acc:0.4f}".format(acc=unqueried_f1_xgb))
    mlflow.log_metric("f1", unqueried_f1_xgb)

    # Now setup nested mlflow runs, using increasing number of observations
    for index in range(N_QUERIES):

        # Log run
        with mlflow.start_run(
            run_name="Active learning, size = {}".format(str(index)), nested=True
        ):
            query_index, query_instance = learner.query(X_pool)

            # Log param and tag
            mlflow.log_param("sample_size", int(index))
            mlflow.set_tag("dataset", "elliptic")

            # Update model with new observation
            X_nq, y_nq = X_pool[query_index].reshape(1, -1), y_pool[
                query_index
            ].reshape(
                1,
            )
            learner.teach(X=X_nq, y=y_nq)

            # Remove observation from pool
            X_pool, y_pool = np.delete(X_pool, query_index, axis=0), np.delete(
                y_pool, query_index
            )

            # Calculate f1 score
            pred = learner.predict(X_test_al)
            model_f1 = f1_score(y_test_al, pred)
            print("F1 after query {n}: {acc:0.4f}".format(n=index + 1, acc=model_f1))

            # Log f1 score
            mlflow.log_metric("f1", model_f1)


# COMMAND ----------

# MAGIC %md Display results directly frm databricks

# COMMAND ----------

df = spark.read.format("mlflow-experiment").load()
display(df.select("metrics.f1", "params.sample_size"))

# COMMAND ----------
