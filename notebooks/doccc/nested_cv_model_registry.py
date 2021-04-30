# Databricks notebook source
# MAGIC %md ## MLflow routine with nested cross validation and periodic scoring/retraining
# MAGIC
# MAGIC In this notebook, we will showcase an example of how to setup a training routine using Mlflow and Structured Streaming.
# MAGIC
# MAGIC We first define a training regime with nested cross validation. We use an inner loop to perform a grid search to select hyperparameters, and than an outer loop on an outer K-fold split to assess general performance. The selected model is then registered.
# MAGIC
# MAGIC We mock a stream, and at every new batch of data, we score the current model, and then retrain and register a new model using the expanded dataset.

# COMMAND ----------

# MAGIC %md ### Imports and settings

# COMMAND ----------

from datetime import datetime

import mlflow.sklearn
import numpy as np
import pandas as pd
import pytz
from mlflow.tracking import MlflowClient
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from sklearn.metrics import confusion_matrix, f1_score
from sklearn.model_selection import GridSearchCV, KFold, cross_val_score
from sklearn.svm import SVC

client = MlflowClient()

# COMMAND ----------

# MAGIC %run ./gold_asif_stream

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# Set path for saving new data
gold_path = "/mnt/public/gold/doccc/mock_stream_register_model_nested_cv"
gold_path_validation = (
    "/mnt/public/gold/doccc/mock_stream_register_model_nested_cv_validation"
)

# Source path
silver_path = "/mnt/public/silver/doccc/doccc"

# Make sure we start from scratch
dbutils.fs.rm(gold_path, True)
dbutils.fs.rm(gold_path_validation, True)

# Set model name
model_name = "GCCCC classifier"

# COMMAND ----------

# MAGIC %md ### Setup training regime

# COMMAND ----------


class GetModelByNestedCv:
    def __init__(
        self,
        options: dict,
        k_inner: int,
        k_outer: int,
        X: pd.DataFrame,
        y: pd.Series,
        metric: str = "f1",
        random_state: int = 111,
    ):
        """Train classifier based on nested cross validation. Sklearn spesific.

        Parameters
        ----------
        options : dict
            Classifier options. Schema
        k_inner : int
            Number of folds for parameter grid search (inner cross validation)
        k_outer : int
            Number of folds for measuring generalized cross validation score
        X : pd.DataFrame
            Full training set explanatory variables
        y : pd.Series
            Full training set response
        metric : str, optional
            Any sklearn metric, by default "f1"
        random_state : int, optional
            Random state for K-fold reproduction, by default 111
        """
        self.options = options
        self.inner_cv = KFold(n_splits=k_inner, shuffle=True, random_state=random_state)
        self.outer_cv = KFold(n_splits=k_outer, shuffle=True, random_state=random_state)
        self.metric = metric
        self.selected_clf = None
        self.generalized_cv = None
        self.X = X
        self.y = y

    @timing
    def select_option_by_nested_cv(self) -> None:
        """
        Select classifier and assosiated parameter grid using
        nested cross validation
        """
        classifiers = self.options
        nested_score = dict()
        for classifier in classifiers:
            clf = GridSearchCV(
                estimator=classifiers[classifier]["clf"],
                param_grid=classifiers[classifier]["param_grid"],
                cv=self.inner_cv,
                scoring=self.metric,
            )
            nested_score[classifier] = np.mean(
                cross_val_score(
                    clf, X=self.X, y=self.y, cv=self.outer_cv, scoring=self.metric
                ).mean()
            )

        self.selected_clf = max(nested_score, key=nested_score.get)
        self.generalized_cv = nested_score[self.selected_clf]

        print(
            f"Selected classifier and parameter grid based on generalized cross validation {self.metric} score ({self.generalized_cv}): {self.selected_clf}"
        )

    @timing
    def train_based_on_selection(self):
        """
        Train based on selected classifier using the same grid search strategy as when
        assessing generalized error, but without outer CV loop
        """
        if self.selected_clf is None:
            print("No classifier selected. Run select_option_by_nested_cv")
            return None

        model_class = self.options[self.selected_clf].get("clf")
        model_class_params = self.options[self.selected_clf].get("param_grid")

        model = GridSearchCV(
            model_class, model_class_params, cv=self.inner_cv, scoring=self.metric
        )
        model.fit(self.X, self.y)

        return model

    def get_generalized_cv_score(self):
        return self.generalized_cv


# COMMAND ----------

# MAGIC  %md ### Train using defined regime and log training using Mlflow (skipped)
# MAGIC
# MAGIC  We go trhrough the code on how to log a run using our training regime. We will reuse most of the code in a retrain/score setting below.

# COMMAND ----------

# Run mlflow experiment and register best model on the fly
"""
# Setup mock stream. By only initializing the class, we write only first_bulk_size data
GMSAL = GoldMockStreamAutoLinear(
    duration_minutes=60,
    full_data=spark.read.format("delta").load(silver_path),
    first_bulk_size=500,
    validation_size=5000,
    gold_path=gold_path,
    batch_size=500,
    validation_path=gold_path_validation,
)
GMSAL.print_config()


with mlflow.start_run() as run:
    mlflow.sklearn.autolog(disable=True)

    df = spark.read.format("delta").load(gold_path)
    pdf = df.toPandas()
    X = pdf.drop(["ID", "Y"], axis=1)
    y = pdf["Y"]

    # Define options
    classifiers = {
        "RandomForest": {
            "clf": RandomForestClassifier(),
            "param_grid": {
                "max_depth": [2, 5, 10, 15, None],
                "criterion": ["gini", "entropy"],
            },
        },
        "AdaBoost": {
            "clf": AdaBoostClassifier(),
            "param_grid": {"n_estimators": [5, 20, 50]},
        },
        "SupportVectorMachine": {
            "clf": SVC(kernel="rbf"),
            "param_grid": {"C": [1, 10, 100], "gamma": [0.01, 0.1]},
        },
        "Dummy": {
            "clf": DummyClassifier(),
            "param_grid": {"strategy": ["most_frequent", "uniform"]},
        },
    }

    metric = "f1"

    # Get data
    df = spark.read.format("delta").load(gold_path)
    pdf = df.toPandas()
    X = pdf.drop(["ID", "Y"], axis=1)
    y = pdf["Y"]

    # Select model/param option
    ModelGetter = GetModelByNestedCv(
        options=classifiers, k_inner=4, k_outer=6, X=X, y=y, metric=metric
    )
    ModelGetter.select_option_by_nested_cv()

    # Train model
    final_model = ModelGetter.train_based_on_selection()

    # Log selections
    mlflow.log_metric(
        f"Generalized CV {metric} error", ModelGetter.get_generalized_cv_score()
    )

    mlflow.log_param("Model class", str(final_model.estimator))
    final_params = final_model.best_estimator_.get_params()
    for param in final_params:
        mlflow.log_param(param, final_params[param])

    # Log model
    artifact_path = "model"
    mlflow.sklearn.log_model(final_model.best_estimator_, artifact_path)

    # Run details
    run_id = run.info.run_uuid
    run_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {run_id}, Run uri: {run_uri}, Experiment ID: {experiment_id}")

    # Register model
    location = f"runs:/{run_id}/{artifact_path}"
    model_details = mlflow.register_model(location, model_name)

    # Add description
    version_desc = f"{str(final_model.estimator)} model, as selected by nested cross validation. See source run for details."
    client.update_model_version(
        name=model_name,
        version=int(model_details.version),
        description=version_desc,
    )
"""

# COMMAND ----------

# MAGIC %md ### Newly arriving data: Stream, score and retrain
# MAGIC
# MAGIC Now, lets assume we regularly receive new data. We want to score this new data and evaluate the performance, and also retrain our model using our enlarged training set. In a real world setting we may not retrain as much as we evaluate.

# COMMAND ----------

# Start data flow
GMSAL = GoldMockStreamAutoLinear(
    duration_minutes=180,
    full_data=spark.read.format("delta").load(silver_path),
    first_bulk_size=200,
    validation_size=3000,
    gold_path=gold_path,
    batch_size=200,
    validation_path=gold_path_validation,
)
GMSAL.print_config()

# COMMAND ----------


def score_new_data(df, model_name):
    """Get scores for new batch"""
    mlflow.sklearn.autolog(disable=True)

    pdf = df.toPandas()
    X_new = pdf.drop(["ID", "Y"], axis=1)
    y_new = pdf["Y"]

    if X.shape[0] == 0:  # No new data
        return None

    # Get latest model version (regardless of stage)
    latest_version = client.get_registered_model(model_name).latest_versions[-1].version
    model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{latest_version}")

    # Get prediction metrix
    ypred = model.predict(X_new)
    f1score = f1_score(y_true=y_new, y_pred=ypred)

    # Return performance
    return f1score


def score_and_retrain(df, batchId):
    with mlflow.start_run() as run:
        # Score last model on new data if model exists
        if model_name in [mod.name for mod in client.list_registered_models()]:
            f1score = score_new_data(df, model_name=model_name)
            mlflow.log_metric("f1_new_data_last_model_version", f1score)

        # Retrain on full dataset
        df = spark.read.format("delta").load(gold_path)  # Re-read
        pdf = df.toPandas()
        X = pdf.drop(["ID", "Y"], axis=1)
        y = pdf["Y"]
        metric = "f1"

        # Set classifiers
        classifiers = {
            "RandomForest": {
                "clf": RandomForestClassifier(),
                "param_grid": {
                    "max_depth": [2, 5, 10, 15, None],
                    "criterion": ["gini", "entropy"],
                    "n_jobs": [4],
                },
            },
            "AdaBoost": {
                "clf": AdaBoostClassifier(),
                "param_grid": {"n_estimators": [5, 10, 20, 50]},
            },
            "SupportVectorMachine": {
                "clf": SVC(kernel="rbf"),
                "param_grid": {"C": [1, 10, 100], "gamma": [0.01, 0.1]},
            },
            "Dummy": {
                "clf": DummyClassifier(),
                "param_grid": {"strategy": ["most_frequent", "uniform"]},
            },
        }

        # Select model/param option
        ModelGetter = GetModelByNestedCv(
            options=classifiers, k_inner=5, k_outer=5, X=X, y=y, metric=metric
        )
        ModelGetter.select_option_by_nested_cv()

        # Train model
        new_model = ModelGetter.train_based_on_selection()

        # Log selections
        mlflow.log_metric(
            f"Generalized CV {metric} error", ModelGetter.get_generalized_cv_score()
        )

        mlflow.log_param("Model class", str(new_model.estimator))
        new_params = new_model.best_estimator_.get_params()
        for param in new_params:
            mlflow.log_param(param, new_params[param])
        mlflow.log_param(
            "Train timestamp",
            datetime.now(pytz.timezone("Europe/Oslo")).strftime("%H:%m:%S"),
        )

        # Log model
        artifact_path = "model"
        mlflow.sklearn.log_model(new_model.best_estimator_, artifact_path)

        # Run details
        run_id = run.info.run_uuid
        run_uri = run.info.artifact_uri
        experiment_id = run.info.experiment_id
        print(f"Run id: {run_id}, Run uri: {run_uri}, Experiment ID: {experiment_id}")

        # Register model
        location = f"runs:/{run_id}/{artifact_path}"
        model_details = mlflow.register_model(location, model_name)

        # Add description
        version_desc = f"{str(new_model.estimator)} model, as selected by nested cross validation. See source run for details."
        client.update_model_version(
            name=model_name,
            version=int(model_details.version),
            description=version_desc,
        )


# Start score/retrain stream. Check for updates every minute
spark.readStream.format("delta").load(gold_path).writeStream.foreachBatch(
    score_and_retrain
).trigger(processingTime="1 minute").start()


# COMMAND ----------

# Start mock data stream which will trigger the score/retrain
GMSAL.start_auto()
