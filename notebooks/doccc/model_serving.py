# Databricks notebook source
# MAGIC %md ## Model deployment using MLflow
# MAGIC
# MAGIC In this introductory demo, we show how a trained model can be published for simple use cases and large scale demands. We start by showing how simple model serving can be performed using MLflow model serving, before moving to enterprise-ready production using Azure Machine Learning with Kubernetes.
# MAGIC
# MAGIC Demo prerequisites:
# MAGIC - Train a `GCCCC classifier` model
# MAGIC - Put one model version into staging
# MAGIC
# MAGIC Note that most of the code below is commented out for failsafing. The cleanup steps are not as straight forward - so avoid running code you are uncertain about

# COMMAND ----------

import re

import azureml
import mlflow.azureml
from azureml.core import Workspace
from azureml.core.compute import AksCompute, ComputeTarget
from azureml.core.webservice import AksWebservice, Webservice
from mlflow.tracking import MlflowClient

# COMMAND ----------

# MAGIC %md ### Early phase: Deploy using Mlflow Serving
# MAGIC
# MAGIC This is by far the quickest way for model serving, and we will show how to do this without a single line of code. It is a convenient and simple way to deploy models for consumption outside the development environment, but it is not recommended for large scale use cases.
# MAGIC
# MAGIC See more on MLflow Model serving [here](https://docs.microsoft.com/en-us/azure/databricks/applications/mlflow/model-serving)
# MAGIC
# MAGIC Note that this way of serving a model is *not* recommended in production. From the documentation:
# MAGIC
# MAGIC *While this service is in preview, Databricks recommends its use for low throughput and non-critical applications. Target throughput is 20 qps and target availability is 99.5%, although no guarantee is made as to either. Additionally, there is a payload size limit of 16 MB per request.*

# COMMAND ----------

# MAGIC %md ### Test/production phase: Deploy using Azure Machine Learning and Azure Kubernetes Services
# MAGIC
# MAGIC A way to deploy the model that *is* recommended in production is through Azure Machine Learning Workspace on an Azure Kubernetes Services cluster. We will go through how to implement this with MLflow here.

# COMMAND ----------

# MAGIC %md #### Step 1: Create Azure ML Workspace
# MAGIC
# MAGIC Note - we would normally do this with ARM-templates, Terraform or pulumi, but showcase the possibilites from Databricks

# COMMAND ----------

workspace_name = "mlw-ai-prod-we-001"
workspace_location = "West Europe"
resource_group = "kantega-aml"
subscription_id = "2920fa54-dd01-4730-b260-e96a1888bcca"


# Outcommented for failsafe - do not run
# unless you know what is going on
""" 
workspace = Workspace.create(name = workspace_name,
                             location = workspace_location,
                             resource_group = resource_group,
                             subscription_id = subscription_id,
                             exist_ok=True)
"""


# COMMAND ----------

# MAGIC %md #### Step 2: Get model image for deployment

# COMMAND ----------

# MAGIC %md Get model details and adjust for Azure workspace deployment

# COMMAND ----------

model_name = "GCCCC classifier"
model_stage = "Production"

model_uri = f"models:/{model_name}/{model_stage}"
azure_description = re.sub(
    "[(,)]",
    "",
    client.get_latest_versions(model_name, stages=[model_stage])[0].description,
)  # No commas or paranthesis
azure_model_name = model_name = re.sub(
    " ", "_", model_name
).lower()  # No spaces, underscore or uppercase

# COMMAND ----------

# MAGIC %md Use mlflow to build a container image

# COMMAND ----------

# Outcommented for failsafe - do not run
# unless you know what is going on
""" 
model_image, azure_model = mlflow.azureml.build_image(model_uri=model_uri, 
                                                      workspace=workspace,
                                                      model_name=azure_model_name,
                                                      image_name=azure_model_name,
                                                      description=azure_description,
                                                      synchronous=False)
"""

# COMMAND ----------

# MAGIC %md #### Step 3: Create AKS Cluster
# MAGIC
# MAGIC Again - we would probably do this outside Databricks.

# COMMAND ----------

# Use the default configuration (you can also provide parameters to customize this)
prov_config = AksCompute.provisioning_configuration()
aks_cluster_name = "aksc-ai-prod-we-001"

# Outcommented for failsafe - do not run
# unless you know what is going on
""" 
aks_target = ComputeTarget.create(workspace = workspace, 
                                  name = aks_cluster_name, 
                                  provisioning_configuration = prov_config)
  

# Wait for the create process to complete
aks_target.wait_for_completion(show_output = True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)
"""

# COMMAND ----------

# MAGIC %md #### Step 4: Deploy on cluster

# COMMAND ----------

# Set configuration and service name
prod_webservice_name = "aksc-ai-prod-we-001"
prod_webservice_deployment_config = AksWebservice.deploy_configuration()

# Outcommented for failsafe - do not run
# unless you know what is going on
""" 
prod_webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = prod_webservice_name,
                                               image = azure_model_image,
                                               deployment_config = prod_webservice_deployment_config,
                                               deployment_target = aks_target)
"""

# COMMAND ----------

# MAGIC %md Congratulations! Your model is deployed in production
