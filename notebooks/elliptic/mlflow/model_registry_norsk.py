# Databricks notebook source
# MAGIC %md # MLflow Model Registry
# MAGIC
# MAGIC Nøkkelelementer
# MAGIC - Model
# MAGIC - Registered model
# MAGIC - Model version
# MAGIC - Model stages
# MAGIC
# MAGIC Vi går gjennom alle disse i denne demoen

# COMMAND ----------

# MAGIC %md ## Model

# COMMAND ----------

# Import og boilerplate
import mlflow
import mlflow.pyfunc
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

with mlflow.start_run() as run:  # Starter tracking

    # Sette i gang autologging
    mlflow.spark.autolog()

    # Hente data
    df = (
        spark.read.format("delta")
        .load("/mnt/public/clean/elliptic")
        .drop("txId", "simple_kr", "real_kr")
        .withColumn("class", col("class").cast(IntegerType()))
    )

    # Transformere data
    feature_cols = [col for col in df.columns if col.startswith("V")]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)
    train = df.filter(col("time") <= 34).drop("time")
    test = df.filter(col("time") > 34).drop("time")

    # Trene modellen
    rf = RandomForestClassifier(labelCol="class", featuresCol="features", maxDepth=5)
    model = rf.fit(train)

    # Logge modellen
    artifact_path = "model"
    mlflow.spark.log_model(model, artifact_path)

    # Logge parametere
    params = model.extractParamMap()
    for item in params:
        mlflow.log_param(item.name, params[item])

    # Beregne og logge metrikk
    predictions = model.transform(test)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="class", predictionCol="prediction", metricName="f1"
    )
    f1score = evaluator.evaluate(predictions)
    mlflow.log_metric("f1", f1score)

    # Print informasjon om kjøringen
    rf_run_id = run.info.run_uuid
    rf_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rf_run_id}, Run uri: {rf_uri}, Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md ## Registered model
# MAGIC
# MAGIC Det finnes to måter for å registrere en modell på i Databricks. Den ene er et grafisk grensesnitt, og den andre er programmatisk via API, som er metoden vi bruker i denne demoen.

# COMMAND ----------

mlflow_model_name = "Supervised model for elliptic data"

# COMMAND ----------

location = f"runs:/{rf_run_id}/{artifact_path}"
model_details = mlflow.register_model(
    location, mlflow_model_name
)  # Modellregistreringen

# COMMAND ----------

# MAGIC %md Legg til beskrivelse

# COMMAND ----------

client.update_registered_model(
    name=model_details.name,
    description="This model uses a classifier to predict the class on the elliptic dataset.",
)

# COMMAND ----------

# MAGIC %md Legg til beskrivelse på gjeldende versjon

# COMMAND ----------

client.update_model_version(
    name=model_details.name,
    version=1,
    description="This model uses RandomForestClassifier from Spark ML, with default parameter values.",
)

# COMMAND ----------

# MAGIC %md Legg til tags

# COMMAND ----------

client.set_registered_model_tag(mlflow_model_name, "dataset", "elliptic")

# COMMAND ----------

# MAGIC %md ## Model version
# MAGIC
# MAGIC La oss nå lage en ny modell for dette datasettet, men med et helt ulikt bibliotek. Vi vil registrere denne nye modellen som en ny versjon til den eksisterende modellen, til tross for at de er bygget helt ulikt.

# COMMAND ----------

with mlflow.start_run() as run:  # Start tracking

    # Sette i gang autologg
    mlflow.xgboost.autolog()

    # Hente data
    df = (
        spark.read.format("delta")
        .load("/mnt/public/clean/elliptic")
        .drop("txId", "simple_kr", "real_kr")
        .withColumn("class", col("class"))
        .toPandas()
    )

    # Transformer med pandas
    X, y = df.drop(columns=["class"]), df[["class", "time"]]
    X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]
    X_train = X_train.drop(columns=["time"])
    X_test = X_test.drop(columns=["time"])
    y_train, y_test = y.loc[(y["time"] <= 34)], y.loc[(y["time"] > 34)]
    y_train = y_train.drop(columns=["time"])
    y_train = y_train.values.ravel().astype(int)
    y_test = y_test.drop(columns=["time"])
    y_test = y_test.values.ravel().astype(int)

    # Trene modell
    xgb_classifier = xgb.XGBClassifier(verbosity=0, use_label_encoder=False)
    model = xgb_classifier.fit(X_train, y_train)

    # Logge modell
    artifact_path = "model"
    mlflow.xgboost.log_model(model, artifact_path)

    # Logge parametre
    params = model.get_xgb_params()
    for item in params:
        mlflow.log_param(item, params[item])

    # Beregne og logge metrikk
    predictions = model.predict(X_test)
    f1score = f1_score(y_test, predictions)
    mlflow.log_metric("f1", f1score)

    # Print info om kjøringen
    rf_run_id = run.info.run_uuid
    rf_uri = run.info.artifact_uri
    experiment_id = run.info.experiment_id
    print(f"Run id: {rf_run_id}, Run uri: {rf_uri}, Experiment ID: {experiment_id}")

    # Registrere modellen "on the fly"
    artifact_path = "model"
    mlflow.xgboost.log_model(
        xgb_model=model,
        artifact_path=artifact_path,
        registered_model_name=mlflow_model_name,
    )

# COMMAND ----------

# MAGIC %md Legg til en beskrivelse for denne modellversjonen

# COMMAND ----------

client.update_model_version(
    name=model_details.name,
    version=2,
    description="This model uses xgboost for classifications, with standard parameter settings",
)

# COMMAND ----------

# MAGIC %md ##Model stages
# MAGIC
# MAGIC Det finnes tre ulike model stages: Staging, Production, and Archived. Vi vil sette en modell i staging, og begynner med modellversjon 1.

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=1, stage="Staging")

# COMMAND ----------

# MAGIC %md Vi kan sette flere versjoner i staging om vi ønsker dette.

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=2, stage="Staging")

# COMMAND ----------

# MAGIC %md La oss nå anta at vi har kjørt versjon 1 en stund, og er overbevist om at denne er klar for produksjon. Veien dit er kort!

# COMMAND ----------

client.transition_model_version_stage(mlflow_model_name, version=1, stage="Production")

# COMMAND ----------

# MAGIC %md ##  Bruk og servering
# MAGIC
# MAGIC Flott, modellene er satt i riktig stage og klar for bruk.
# MAGIC
# MAGIC Hvordan gjør vi det?

# COMMAND ----------

# Velg modell og stadie
model_name = mlflow_model_name
stage = "Staging"

# Hente modell
model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{stage}")

# Bruk modell. Her bruker vi eksisterende data, men hva som helst som oppfyller signaturen kan mates inn
model.predict(X_test)

# COMMAND ----------

# MAGIC %md Det er også enkelt å servere scoring på tilsvarende måte gjennom et REST API. Se mer [her](https://docs.databricks.com/applications/mlflow/model-serving.html#score-via-rest-api-request).
