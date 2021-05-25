# Databricks notebook source
# MAGIC %md #MLFlow projects concept example
# MAGIC
# MAGIC In this demo, we will package the code for running an MLModel as a project. There are several good reasons to package projects
# MAGIC
# MAGIC
# MAGIC 1. Dependencies
# MAGIC   Projects depend on various libraries, and provde an environment along with the model building is very useful for simple runs in various settings
# MAGIC 2. Complexity
# MAGIC   As time passes, ML Projects tend to become increasingly complex. Collecting all steps in a common     structure is useful
# MAGIC 3. Lineage
# MAGIC   Having the full pipeline at hand, it is simpler to trace failures
# MAGIC
# MAGIC The basic setup for an MLflow project ia as follows
# MAGIC
# MAGIC ```
# MAGIC my_ml_project/
# MAGIC |-- MLproject
# MAGIC |-- conda.yaml
# MAGIC |-- main.py
# MAGIC |-- {additional code}.py
# MAGIC ```
# MAGIC
# MAGIC The core component and orchestrator in MLflow projects is the `MLproject file`. This contains pointers to the remaining components. The `conda.yaml` file contains the environment settings, and `main.py` is backed by `train.py`. The setup will become more clear as we get into the example.

# COMMAND ----------

# MAGIC %md ### Create cli-runnable model using [click](https://click.palletsprojects.com/en/7.x/)

# COMMAND ----------

# pip install delta-lake-reader[azure]

# COMMAND ----------

import click
import mlflow
import mlflow.xgboost
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import f1_score, recall_score


@click.command()
@click.option("--data_path", default="/mnt/public/clean/elliptic", type=str)
@click.option("--max_depth", default=6, type=int)
def mlflow_xgb(data_path, max_depth):
    with mlflow.start_run() as run:
        # Import data
        df = (
            DeltaTable("/dbfs" + data_path)
            .to_pandas()
            .drop(columns=["txId", "simple_kr", "real_kr"])
        )
        X, y = df.drop(columns=["class"]), df[["class", "time"]]
        X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]
        X_train = X_train.drop(columns=["time"])
        X_test = X_test.drop(columns=["time"])
        y_train, y_test = y.loc[(y["time"] <= 34)], y.loc[(y["time"] > 34)]
        y_train = y_train.drop(columns=["time"])
        y_train = y_train.values.ravel().astype(int)
        y_test = y_test.drop(columns=["time"])
        y_test = y_test.values.ravel().astype(int)
        # Fit model
        xgb_classifier = xgb.XGBClassifier(max_depth=max_depth, verbosity=0)
        xgb_classifier.fit(X_train, y_train)

        # Make predictions
        predictions = xgb_classifier.predict(X_test)

        # Log param
        mlflow.log_param("max_depth", max_depth)

        # Log metrics
        f1score_xgb = f1_score(y_test, predictions)
        mlflow.log_metric("f1", f1score_xgb)
        recall_xgb = recall_score(y_test, predictions)
        mlflow.log_metric("recall", recall_xgb)


# COMMAND ----------

# MAGIC %md Check that the code is runnable by cli input

# COMMAND ----------

from click.testing import CliRunner

runner = CliRunner()
result = runner.invoke(mlflow_xgb, ["--max_depth", 4], catch_exceptions=True)
assert result.exit_code == 0, "Code failed"  # Check to see that it worked
print("Success!")

# COMMAND ----------

# MAGIC %md ### Add the nice wrapping
# MAGIC We will now add the code needed to wrap the runnable model. This may seem a little strange, since we will actually just hard code file contents to mounted storage. It makes more sense to do it in your favorite environment, but for the sake of including the entire work process, we will use Databricks

# COMMAND ----------

# Set the directory
train_path = "/mnt/public/models/xgb_demo/"

# COMMAND ----------

# MAGIC %md #### Create MLproject file

# COMMAND ----------

# Use dbfs put to save the MLProject file
file_content = """
name: XGB-classifier-elliptic demo 

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      data_path: {type: str, default: "/mnt/public/clean/elliptic"}
      max_depth: {type: int, default: 6}
    command: "python train.py --data_path {data_path} --max_depth {max_depth}"
""".strip()

# Save to storage
dbutils.fs.put(
    train_path + "MLproject", file_content, overwrite=True
)  # Feel free to check the data lake - was it stored as expected?

print(file_content)

# COMMAND ----------

# MAGIC %md #### Create conda file

# COMMAND ----------

"""
Get current version, must ensure having imports
import sklearn
import pandas
import xgboost
import mlflow

{package name}.__version
"""

file_content = f"""
name: XGB-classifier-elliptic-demo 
channels:
  - defaults
dependencies:
  - pandas=1.1.0
  - scikit-learn=0.23.2
  - pip
  - pip:
    - mlflow
    - xgboost
    - delta-lake-reader[azure]
""".strip()

dbutils.fs.put(train_path + "conda.yaml", file_content, overwrite=True)

print(file_content)

# COMMAND ----------

# MAGIC %md #### Create train.py

# COMMAND ----------

file_content = """
import click
import mlflow
import mlflow.xgboost
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import f1_score, recall_score

@click.command()
@click.option("--data_path", default="/mnt/public/clean/elliptic", type=str)
@click.option("--max_depth", default=6, type=int)
def mlflow_xgb(data_path, max_depth):
    with mlflow.start_run() as run:
        # Import data
        df = DeltaTable("/dbfs"+data_path).to_pandas().drop(columns=["txId", "simple_kr", "real_kr"])
        X, y = df.drop(columns=["class"]), df[["class", "time"]]
        X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]
        X_train = X_train.drop(columns=["time"])
        X_test = X_test.drop(columns=["time"])
        y_train, y_test = y.loc[(X["time"] <= 34)], y.loc[(X["time"] > 34)]
        y_train = y_train.drop(columns=["time"])
        y_train = y_train.values.ravel().astype(int)
        y_test = y_test.drop(columns=["time"])
        y_test = y_test.values.ravel().astype(int)
        # Fit model
        xgb_classifier = xgb.XGBClassifier(max_depth=max_depth, verbosity=0)
        xgb_classifier.fit(X_train, y_train)

        # Make predictions
        predictions = xgb_classifier.predict(X_test)

        # Log param
        mlflow.log_param("max_depth", max_depth)

        # Log metrics
        f1score_xgb = f1_score(y_test, predictions)                                               
        mlflow.log_metric("f1", f1score_xgb)
        recall_xgb = recall_score(y_test, predictions)
        mlflow.log_metric("recall", recall_xgb)
        
if __name__ == "__main__":
  mlflow_xgb() # Note that this does not need arguments thanks to click
""".strip()

dbutils.fs.put(train_path + "train.py", file_content, overwrite=True)
print(file_content)

# COMMAND ----------

# MAGIC %md ### Check directory

# COMMAND ----------

dbutils.fs.ls(train_path)

# COMMAND ----------

# MAGIC %md ### Run the project
# MAGIC This should work even with cleared state

# COMMAND ----------

import mlflow

model_path = "/dbfs/mnt/public/models/xgb_demo/"
mlflow.projects.run(
    uri=model_path,
    parameters={
        "max_depth": 3,
    },
)

# COMMAND ----------

mlflow.end_run()
