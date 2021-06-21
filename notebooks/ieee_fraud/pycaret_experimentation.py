# Databricks notebook source
# MAGIC %md ##Low Code ML
# MAGIC #### PyCaret + MLflow experimenting

# COMMAND ----------

# MAGIC %md Install missing pips and import libraries

# COMMAND ----------

# MAGIC %pip install pycaret=="2.3.1"

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from pycaret.classification import *

client = MlflowClient()
import mlflow.sklearn

# COMMAND ----------

# MAGIC %md Spark <-> Pandas speedup

# COMMAND ----------

# Enable pyarrow for optimizing spark df -> pandas
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Clear cache, we need all memory we can get..
spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md Data settings

# COMMAND ----------

identity_path = "/mnt/public/silver/ieee_fraud/identity/"
transactions_path = "/mnt/public/silver/ieee_fraud/transaction/"
notebook_path = "/Shared/ieee_fraud/gold_pycaret_dupe"  # For mlflow UI
model_name = "PyCaret IEEE fraud sample model"

# COMMAND ----------

# MAGIC %md Get training data. We sample 10% since the dataset is pretty large for a tiny cluster like ours.

# COMMAND ----------

# Get identity data
dfi = spark.read.format("delta").load(identity_path)

# Get transactions data. Sampling half.
dft = spark.read.format("delta").load(transactions_path).sample(0.1)

# Spark join frames
df = dft.join(dfi, on="transaction_id", how="inner")

# Convert to Pandas. Send all the data to the driver. Spark and Pycaret are not best friends just yet
pdf = df.toPandas()

# COMMAND ----------

# MAGIC %md ### Enter Pycaret

# COMMAND ----------

# MAGIC %md We start with the setup
# MAGIC
# MAGIC A lot of things are going on under the hood here
# MAGIC
# MAGIC Our call does the following
# MAGIC - Sets target variable
# MAGIC - Tells pycaret to log the experiment - mlflow behind the scenes
# MAGIC - Need to set experiment name same as notebook name to enable pretty notebook UI for MLflow
# MAGIC - Setting silent True removes the need to confirm the planned transformations
# MAGIC - Setting HTML false removes the need to print stuff not suited to print in Databricks
# MAGIC
# MAGIC Also, among other things, the following is performed by default
# MAGIC - Missing categorical features are set to a constant ‘not_available’
# MAGIC - Missing numerical data is imputed by mean
# MAGIC - One hot encoding is performed on categorical features
# MAGIC - Data types are inferred (ie. categorical features)
# MAGIC
# MAGIC Finally all ojects are stored in environment for future use within the pycaret framework (i.e preprocessed dataframe)

# COMMAND ----------

clf = setup(
    pdf,
    target="is_fraud",
    log_experiment=True,
    experiment_name=notebook_path,
    silent=True,
    html=False,
)

# COMMAND ----------

# MAGIC %md Use the compare_models, which trains candidates with default parameters

# COMMAND ----------

top5 = compare_models(sort="F1", turbo=True, budget_time=30, n_select=5)
best = top5[0]

# COMMAND ----------

# MAGIC %md A sample of different plots out of the box

# COMMAND ----------

# Plot performance by ROC curves
plot_model(best)

# COMMAND ----------

# Plot feature importance
plot_model(best, "feature")

# COMMAND ----------

# MAGIC %md Interpret by shap

# COMMAND ----------

interpret_model(best)

# COMMAND ----------

# MAGIC %md Tune the final model.
# MAGIC
# MAGIC Optimally, this would be performed as a nested CV within each fold of the compare_models procedure, resulting in (n models) * (k outer folds) * (m hyperparameter combination candidates) * (k2 hyperparameter CV inner folds) model fits. The pycaret documentation does however suggests tuning after comparing model classes, resulting in (n models) * (k outer folds) + (m hyperparameter combination candidates) * (k3 hyperparameter CV folds). Meaning we trade away the exhaustive optima search for computational feasibility.

# COMMAND ----------

tuned = tune_model(best, fold=5)

# COMMAND ----------

# MAGIC %md Finalize the model

# COMMAND ----------

final_model = finalize_model(tuned)

# COMMAND ----------

# MAGIC %md ### Register the model in Mlflow Model Registry

# COMMAND ----------

# Register the model. This could probably be written more elegantly. Here, we just use the latest timestamp
exp = client.get_experiment_by_name(
    notebook_path
)
runs = client.list_run_infos(exp.experiment_id)
start_times = [run.start_time for run in runs]
final_run_id = runs[start_times.index(max(start_times))].run_id

result = mlflow.register_model(
    f"runs:/{final_run_id}/model", "PyCaret Ieee fraud sample model"
)

# COMMAND ----------

# MAGIC %md Use the registered model (mostly) new dirty data

# COMMAND ----------

# Get identity data
dfi_test = spark.read.format("delta").load(identity_path)

# Get sample transactions data, smaller sample, hopefully not overlapped (could assert but leave it to probabilities for now)
dft_test = (
    spark.read.format("delta")
    .load(transactions_path)
    .sample(0.01)
)

# Spark join frames
df_test = dft_test.join(dfi_test, on="transaction_id", how="inner")

# Convert to Pandas. Send all the data to the driver
pdf_test = df_test.toPandas()

# COMMAND ----------

model_version = 1

model = mlflow.sklearn.load_model(model_uri=f"models:/{model_name}/{model_version}")

scores = model.predict_proba(pdf_test)
predicted_labels = model.predict(pdf_test)

# COMMAND ----------

# MAGIC %md Check performance

# COMMAND ----------

from sklearn.metrics import accuracy_score, f1_score, recall_score, roc_auc_score

roc_auc_score(y_true=pdf_test["is_fraud"], y_score=scores[:, 1]), f1_score(
    pdf_test["is_fraud"], predicted_labels
), accuracy_score(pdf_test["is_fraud"], predicted_labels), recall_score(
    pdf_test["is_fraud"], predicted_labels
)
