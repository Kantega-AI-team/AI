# Databricks notebook source
import mlflow.sklearn
import numpy as np
import pandas as pd
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

# Make sure we start from scratch
dbutils.fs.rm(gold_path, True)

# Set model name
model_name = "GCCCC classifier"

# Initialize data
n_observations = 1000
GMS = GoldMockStream(
    full_data=spark.read.format("delta").load("/mnt/public/silver/doccc/doccc"),
    first_bulk_size=n_observations,
    validation_size=15000,
    num_steps=5,
    gold_path=gold_path,
    validation_path="/mnt/public/gold/doccc/mock_stream_register_model_nested_cv_validation",
)

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

# Run mlflow experiment and register best model on the fly
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
                "max_depth": [2, 5, 10, None],
                "n_estimators": [50, 100, 200],
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
        name=model_name, version=int(model_details.version), description=version_desc
    )

# COMMAND ----------

# MAGIC %md Now, lets use a stream of incoming new data
# MAGIC
# MAGIC Our tactic is as follows:
# MAGIC
# MAGIC - We first train the model using nested cross validation for selection
# MAGIC - Every X minutes, we ingest new data. At each ingest we
# MAGIC   - Predict outcomes and log performance using new labels
# MAGIC   - Retrain the model using excactly the same routine - Watch out for leakage!

# COMMAND ----------


def score_new_data(df):
    # TODO write scoring and logging
    pass


def retrain():
    # TODO write retraining with registerin
    pass


def score_and_retrain(df, batchId):
    # TODO implement score and retrain
    pass


# GoldMockStreamAutoLinear().start_auto()
# spark.readStream.format("delta").load(gold_path).writeStream.foreachBatch(score_and_retrain).trigger(minutes="XX").start()

# COMMAND ----------
