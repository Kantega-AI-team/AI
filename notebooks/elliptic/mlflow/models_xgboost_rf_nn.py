# Databricks notebook source
# MAGIC %md # MLflow Models concept example
# MAGIC 
# MAGIC MLflow models is a convenient way to package machine learning models to allow usage in a variety of downstream tools. It defines a "flavor" convention, that is understandable by different tools. We have actually already used this concept in the tracking demo, since the function `log_model` was used at some points. This example goes into more details about the core of the MLflow Models concept: **flavors**.
# MAGIC 
# MAGIC The basic setup of an MLFlow model is as follows
# MAGIC 
# MAGIC ```
# MAGIC some_model/
# MAGIC |-- MLmodel
# MAGIC |-- model.pkl
# MAGIC ```
# MAGIC 
# MAGIC and the `MLmodel` describes flavors, for example
# MAGIC 
# MAGIC ```
# MAGIC time_created: 2018-05-25T17:28:53.35
# MAGIC 
# MAGIC flavors:
# MAGIC   sklearn:
# MAGIC     sklearn_version: 0.19.1
# MAGIC     pickled_model: model.pkl
# MAGIC   python_function:
# MAGIC     loader_module: mlflow.sklearn
# MAGIC ```
# MAGIC 
# MAGIC These flavors represents different ways to interact with the models. The built in flavors include
# MAGIC 
# MAGIC ```
# MAGIC Python Function (python_function)
# MAGIC R Function (crate)
# MAGIC H2O (h2o)
# MAGIC Keras (keras)
# MAGIC MLeap (mleap)
# MAGIC PyTorch (pytorch)
# MAGIC Scikit-learn (sklearn)
# MAGIC Spark MLlib (spark)
# MAGIC TensorFlow (tensorflow)
# MAGIC ONNX (onnx)
# MAGIC MXNet Gluon (gluon)
# MAGIC XGBoost (xgboost)
# MAGIC LightGBM (lightgbm)
# MAGIC Spacy(spaCy)
# MAGIC Fastai(fastai)
# MAGIC Statsmodels (statsmodels)
# MAGIC ```
# MAGIC 
# MAGIC but you can also add your own custom flavors, with custom classes. Any model built in python can be served using `python_function`, which we will also look into. 

# COMMAND ----------

# MAGIC %md ### Demonstrating flavors

# COMMAND ----------

# MAGIC %md We first get and test/train split our data

# COMMAND ----------

df = (
    spark.read.load("/mnt/public/clean/elliptic")
    .toPandas()
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

# COMMAND ----------

# MAGIC %md We will start by training an xgboost model using the `elliptic` dataset. We use MLflow tracking for keeping account of the run, but we skip the loggin. We will also log a signature, which is convenient in practice - it tells you what input is expected and which output it gives.

# COMMAND ----------

import mlflow
import mlflow.xgboost
import xgboost as xgb
from mlflow.models.signature import infer_signature
from sklearn.metrics import f1_score, recall_score

with mlflow.start_run(run_name="XGB Model") as run:
    # Fit model
    xgb_classifier = xgb.XGBClassifier(verbosity=0)
    xgb_classifier.fit(X_train, y_train)

    # Make predictions
    predictions = xgb_classifier.predict(X_test)

    # Log metrics
    f1score_xgb = f1_score(y_test, predictions)
    mlflow.log_metric("f1", f1score_xgb)

    # Log the model
    mlflow.xgboost.log_model(
        xgb_classifier, "model", signature=infer_signature(X_test, predictions)
    )

    # Run info
    xgb_run_id = run.info.run_uuid
    xgb_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {xgb_run_id}, Run uri: {xgb_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md We will also train a random forest model, using the same data

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

with mlflow.start_run(run_name="RF Model") as run:
    # Fit model
    rf_classifier = RandomForestClassifier()
    rf_classifier.fit(X_train, y_train)

    # Make predictions
    predictions = rf_classifier.predict(X_test)

    # Log metrics
    f1score_rf = f1_score(y_test, predictions)
    mlflow.log_metric("f1", f1score_rf)

    # Log the model
    mlflow.sklearn.log_model(
        rf_classifier, "model", signature=infer_signature(X_test, predictions)
    )

    # Run info
    rf_run_id = run.info.run_uuid
    rf_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rf_run_id}, Run uri: {rf_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md And also a tensorflow model

# COMMAND ----------

import mlflow.keras
import numpy as np
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential

with mlflow.start_run(run_name="NN Model") as run:
    # Fit model
    nn_classifier = Sequential(
        [Dense(50, input_dim=165, activation="relu"), Dense(1, activation="sigmoid")]
    )
    nn_classifier.compile(loss="binary_crossentropy", optimizer="adam")
    nn_classifier.fit(X_train, y_train, validation_split=0.2, epochs=10, verbose=0)

    # Make predictions
    predictions = nn_classifier.predict(X_test)

    # Log metrics
    f1score_nn = f1_score(y_test, np.round(predictions))
    mlflow.log_metric("f1", f1score_nn)

    # Log the model
    mlflow.keras.log_model(
        nn_classifier, "model", signature=infer_signature(X_test, predictions)
    )

    # Run info
    nn_run_id = run.info.run_uuid
    nn_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {nn_run_id}, Run uri: {nn_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md ### Using a custom model with `pyfunc`
# MAGIC 
# MAGIC See the [docs](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#pyfunc-create-custom) for more on custom Pyfunc models

# COMMAND ----------

X_test.shape[0]

# COMMAND ----------

import mlflow.pyfunc


class RandomGivenP(mlflow.pyfunc.PythonModel):
    """
    Really useless model for two class models
    Set the probability of the first class to some guess
    Draw all predictions randomly given the probability of the classes

    Simple example, note that this class' predict function will not
    work out of the box using predict after mlflow.load_model,
    because some additional boilerplate
    [load_context, context as predict input]
    would be needed.
    """

    def __init__(self, p):
        assert 0 < p < 1, "p must be between 0 and 1"
        self.p = p

    def predict(self, model_input):
        n = model_input.shape[0]
        # Just a random draw
        return np.random.choice(a=[0, 1], size=n, p=[self.p, 1 - self.p])


with mlflow.start_run(run_name="RGP Model") as run:
    # Fit model
    rgp_classifier = RandomGivenP(p=0.5)

    # Make predictions
    predictions = rgp_classifier.predict(X_test)

    # Log metrics
    f1score_rgp = f1_score(y_test, predictions)
    mlflow.log_metric("f1", f1score_rgp)

    # Log the model
    mlflow.pyfunc.log_model(python_model=rgp_classifier, artifact_path="model")

    # Run info
    rgp_run_id = run.info.run_uuid
    rgp_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rgp_run_id}, Run uri: {rgp_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md ### Loading and using models

# COMMAND ----------

# MAGIC %md Load all models using the same syntax and predict for a sample

# COMMAND ----------

xgb_pyfunc_model = mlflow.pyfunc.load_model(model_uri=f"{xgb_uri}/model")
xgb_xgboost_model = mlflow.xgboost.load_model(model_uri=f"{xgb_uri}/model")
rf_pyfunc_model = mlflow.pyfunc.load_model(model_uri=f"{rf_uri}/model")
rf_sklearn_model = mlflow.sklearn.load_model(model_uri=f"{rf_uri}/model")
nn_pyfunc_model = mlflow.pyfunc.load_model(model_uri=f"{nn_uri}/model")
nn_keras_model = mlflow.keras.load_model(model_uri=f"{nn_uri}/model")
rgp_pyfunc_model = mlflow.pyfunc.load_model(model_uri=f"{rgp_uri}/model")

# COMMAND ----------

# MAGIC %md All pyfunc flavors can be used in the same way

# COMMAND ----------

sample = X_test.sample(n=5)
xgb_pyfunc_model.predict(sample)
rf_pyfunc_model.predict(sample)
nn_pyfunc_model.predict(sample)

# COMMAND ----------

list(rf_pyfunc_model.predict(sample))

# COMMAND ----------

# MAGIC %md Different flavors

# COMMAND ----------

type(rf_pyfunc_model)

# COMMAND ----------

type(rf_sklearn_model)

# COMMAND ----------

# MAGIC %md With the same result

# COMMAND ----------

pred1 = xgb_pyfunc_model.predict(sample)
pred2 = xgb_xgboost_model.predict(xgb.DMatrix(sample.values))
pred1 == pred2

# COMMAND ----------

# MAGIC %md If you want to learn more about the MLflow models concept, read the [docs](https://www.mlflow.org/docs/latest/models.html).