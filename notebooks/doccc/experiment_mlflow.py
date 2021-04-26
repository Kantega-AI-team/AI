# Databricks notebook source
import matplotlib.pyplot as plt
import mlflow
from sklearn.linear_model import LassoCV

# COMMAND ----------

# MAGIC %run ./gold_asif_stream

# COMMAND ----------

silver_path = "/mnt/public/silver/doccc/doccc"
gold_path = "/mnt/public/gold/doccc/mock_stream"
validation_path = "/mnt/public/gold/doccc/mock_stream_validation"
silver_data = spark.read.format("delta").load(silver_path)

with mlflow.start_run() as run:
    GMS = GoldMockStream(
        full_data=silver_data,
        first_bulk_size=5000,
        validation_size=25000,
        gold_path=gold_path,
        validation_path=validation_path,
        num_steps=1,
    )
    df = spark.read.format("delta").load(gold_path)
    X = df.select(
        [column for column in df.columns if column.startswith("X")]
    ).toPandas()
    y = df.select("Y").toPandas().to_numpy().ravel()

    mlflow.sklearn.autolog()

    # Train a basic classifier - lasso regression using 5-fold cross validation
    # with iterative fitting along regularization path. Model selection
    # is done by cross validation. Lasso provides a neat use case for 
    # showcasing model selection as coefficients will shrink to
    # exactly zero due to the L1 norm of the coefficient vector
    # used as (part) of the penalty score.
    # See help(LassoCV) or sklearn docs for further details
    reg = LassoCV(cv=5, random_state=0).fit(X, y)
    params = reg.get_params()
    for key in params:
        mlflow.log_param(key, params.get(key))

    # Explicit metric logging not needed - autologger keeps track of R2, MSE, MAE
    # mlflow.log_metric("R2", reg.score(X,y))

    # Plot chosen coefficients using seaborn and save using mlflow
    desc = (
        spark.read.format("delta")
        .load("/mnt/public/silver/doccc/field_descriptions")
        .select([column for column in df.columns if column.startswith("X")])
        .collect()[0]
    )
    desc = [desc[i] for i in range(len(desc))]
    a4_dims = (25, 4)
    fig, ax = pyplot.subplots(figsize=a4_dims)
    barplot = sns.barplot(x=desc, y=reg.coef_, ax=ax)
    mlflow.log_figure(fig, "coef_plot.png")
