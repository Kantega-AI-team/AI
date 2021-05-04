# MLflow

![ml architecture](/docs/images/MLflow-logoS.jpeg)

MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It has the following primary components:

* Tracking: Allows you to track experiments to record and compare parameters and results.
* Models: Allow you to manage and deploy models from a variety of ML libraries to a variety of model serving and inference platforms.
* Projects: Allow you to package ML code in a reusable, reproducible form to share with other data scientists or transfer to production.
* Model Registry: Allows you to centralize a model store for managing models’ full lifecycle stage transitions: from staging to production, with capabilities for versioning and annotating.
* Model Serving: Allows you to host MLflow Models as REST endpoints.

## Notebook example

In this Notebook example we are using the Elliptic data set, creating two different models, log their parameters and metrics (F1-score),
putting both in staging and finally one of them in production. At last it's shown how to score a test set using a registered model and a link
for serving a model using a REST API.

[**Notebook - MLflow**](/notebooks/elliptic/mlflow/model_registry.py)
