# Databricks notebook source
# MAGIC %md # MLflow Model Registry concepts
# MAGIC
# MAGIC The MLflow Model registry is a model repository which allows management of the full lifecycle of models. The registry also includes a set of APIs and a pretty nice UI. There are options to comment on models and set up notifications.
# MAGIC
# MAGIC There are huge benefits providing a model registry in any ML solution. It provides control, linage, monitoring, versioning and simple stage transitioning.
# MAGIC
# MAGIC The key concepts within the model registry are:
# MAGIC - Model
# MAGIC - Registered model
# MAGIC - Model version
# MAGIC - Model stages
# MAGIC
# MAGIC We will go through all of these in the following demo

# COMMAND ----------

# MAGIC %md ## Model
# MAGIC
# MAGIC We have seen this part before. Again, we will use the elliptic dataset and log a model using MLflow tracking. This time, however, we fit a spark model.

# COMMAND ----------

import mlflow.sklearn
import mlflow.spark
import xgboost as xgb
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from sklearn.metrics import f1_score

client = MlflowClient()

# COMMAND ----------

with mlflow.start_run() as run:
    mlflow.spark.autolog()

    # Get and structure data
    df = (
        spark.read.format("delta")
        .load("/mnt/public/clean/elliptic")
        .drop("txId", "simple_kr", "real_kr")
        .withColumn("class", col("class").cast(IntegerType()))
    )
    feature_cols = [col for col in df.columns if col.startswith("V")]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)
    train = df.filter(col("time") <= 34).drop("time")
    test = df.filter(col("time") > 34).drop("time")

    # Train model
    rf = RandomForestClassifier(labelCol="class", featuresCol="features", maxDepth=5)
    model = rf.fit(train)

    # Get input and output schema
    input_schema = train.select("features").schema
    output_schema = train.select("class").schema

    # Log model. Set the model path to "model", it is convenient
    # to use the same convention for different models,
    # it will be put in a run-specific place anyway
    artifact_path = "model"
    mlflow.spark.log_model(model, artifact_path)

    # Log params
    params = model.extractParamMap()
    for item in params:
        mlflow.log_param(item.name, params[item])

    # Metrics
    predictions = model.transform(test)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="class", predictionCol="prediction", metricName="f1"
    )
    f1score = evaluator.evaluate(predictions)
    mlflow.log_metric("f1", f1score)

    # Run info
    rf_run_id = run.info.run_uuid
    rf_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rf_run_id}, Run uri: {rf_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md ## Register the model
# MAGIC
# MAGIC There are two ways to log the model in databricks. One is to use the graphical interface in the experiments UI. You simply enter the model file tree and a button to register the model will appear. We will use the second approach, by using the API.

# COMMAND ----------

mlflow_model_name = "Supervised model for elliptic data"

# COMMAND ----------

location = f"runs:/{rf_run_id}/{artifact_path}"
model_details = mlflow.register_model(location, mlflow_model_name)

# COMMAND ----------

# MAGIC %md Add descriptions to the whole model using the API

# COMMAND ----------

client.update_registered_model(
    name=model_details.name,
    description="This model uses a classifier to predict the class on the elliptic dataset.",
)

# COMMAND ----------

# MAGIC %md Add descriptions to specific versions

# COMMAND ----------

client.update_model_version(
    name=model_details.name,
    version=1,
    description="This model uses RandomForestClassifier from Spark ML, with default parameter values.",
)

# COMMAND ----------

# MAGIC %md You can also set tags

# COMMAND ----------

client.set_registered_model_tag(mlflow_model_name, "dataset", "elliptic")
# You can add version specific tags with set_model_version_tag

# COMMAND ----------

# MAGIC %md Of course, all of the above steps on registering can be performed using the UI. Check out the UI now. Click the ![models](https://docs.databricks.com/_images/models-icon.png) icon in the sidebar

# COMMAND ----------

# MAGIC %md ## Different model versions
# MAGIC
# MAGIC Lets now create a similar model, but use the xgboost classifier we have seen previously. Instead of calling this an entirely new model, we will simply make it a version of the supervised learning model.

# COMMAND ----------

with mlflow.start_run() as run:
    mlflow.xgboost.autolog()

    # Get and structure data
    df = (
        spark.read.format("delta")
        .load("/mnt/public/clean/elliptic")
        .drop("txId", "simple_kr", "real_kr")
        .withColumn("class", col("class"))
        .toPandas()
    )

    # Pandas setup
    X, y = df.drop(columns=["class"]), df[["class", "time"]]
    X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]
    X_train = X_train.drop(columns=["time"])
    X_test = X_test.drop(columns=["time"])
    y_train, y_test = y.loc[(y["time"] <= 34)], y.loc[(y["time"] > 34)]
    y_train = y_train.drop(columns=["time"])
    y_train = y_train.values.ravel().astype(int)
    y_test = y_test.drop(columns=["time"])
    y_test = y_test.values.ravel().astype(int)

    # Train model
    xgb_classifier = xgb.XGBClassifier(verbosity=0, use_label_encoder=False)
    model = xgb_classifier.fit(X_train, y_train)

    # Log model. Set the model path to "model", it is convenient
    # to use the same convention for different models,
    # it will be put in a run-specific place anyway
    artifact_path = "model"
    mlflow.xgboost.log_model(model, artifact_path)

    # Log params
    params = model.get_xgb_params()
    for item in params:
        mlflow.log_param(item, params[item])

    # Metrics
    predictions = model.predict(X_test)
    f1score = f1_score(y_test, predictions)
    mlflow.log_metric("f1", f1score)

    # Run info
    rf_run_id = run.info.run_uuid
    rf_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rf_run_id}, Run uri: {rf_uri}, Experiment ID: {experiment_id}")

    # Lets just register the model as we log it! We specify the same name as for the rf model
    artifact_path = "model"
    mlflow.xgboost.log_model(
        xgb_model=model,
        artifact_path=artifact_path,
        registered_model_name=mlflow_model_name,
    )

# COMMAND ----------

# MAGIC %md Lets also add a description

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
client.update_model_version(
    name=model_details.name,
    version=2,
    description="This model uses xgboost for classifications, with standard parameter settings",
)

# COMMAND ----------

# MAGIC %md Check out the models tab again. We now have two completely different models with different version added as the same model class.

# COMMAND ----------

# MAGIC %md ##Model stages
# MAGIC
# MAGIC Another really neat thing about MLFlow is model stages. There are three levels: Staging, Production, and Archived. Lets first set our spark random forest classifier in staging

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=1, stage="Staging")

# COMMAND ----------

# MAGIC %md Now take a look at the models page again. You will see that our model has been put in "staging". Congratulations! We can have several models in staging. Lets also add the xgboost model into staging.

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=2, stage="Staging")

# COMMAND ----------

# MAGIC %md Let us now assume that the model has been running, monitored and known to be a decent candidate. We set the rf model in production.

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=1, stage="Production")

# COMMAND ----------

# MAGIC %md ##  Usage and serving
# MAGIC
# MAGIC So we now have our model registry neatly set up, all code is tracked and we could find all the underlying data using delta parquet time travel. But how do we use the models?
# MAGIC
# MAGIC First, lets assume we are in Databricks, or an environment with mlflow server access. Lets load our staging model and use it to predict on `X_test`. This could of course be any dataset of the right shape.

# COMMAND ----------

import mlflow.pyfunc

model_name = mlflow_model_name
stage = "Staging"

model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{stage}")
model.predict(X_test)

# COMMAND ----------

# MAGIC %md We could also serve the model using a REST API. See more in the [docs](https://docs.databricks.com/applications/mlflow/model-serving.html#score-via-rest-api-request).

# COMMAND ----------

# MAGIC %md #### Deleting an entire model
# MAGIC
# MAGIC Deletion requires archiving all versions to ensure no models are in an active stage

# COMMAND ----------

"""
client.transition_model_version_stage(mlflow_model_name, version=1, stage="Archived")
client.transition_model_version_stage(mlflow_model_name, version=2, stage="Archived")
client.delete_registered_model(name=mlflow_model_name)
"""
