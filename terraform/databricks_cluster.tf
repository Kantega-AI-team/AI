data "databricks_spark_version" "latest" {
  ml         = true
  depends_on = [azurerm_databricks_workspace.dbw]
}

resource "databricks_cluster" "dbwc" {
  cluster_name            = var.databricks_cluster_name
  spark_version           = data.databricks_spark_version.latest.id
  node_type_id            = var.databricks_cluster_node_type
  autotermination_minutes = 20
  num_workers             = 0

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  library {
    pypi {
      package = "dgl"
    }
  }
  library {
    pypi {
      package = "delta-lake-reader"
    }
  }
  library {
    pypi {
      package = "keras"
    }
  }
  library {
    cran {
      package = "leaflet"
    }
  }
  library {
    cran {
      package = "here"
    }
  }
  library {
    pypi {
      package = "shap"
    }
  }
  library {
    pypi {
      package = "modAL"
    }
  }
  library {
    pypi {
      package = "networkx"
    }
  }
  library {
    pypi {
      package = "adlfs"
    }
  }
  library {
    pypi {
      package = "azureml-sdk[databricks]"
    }
  }
  library {
    cran {
      package = "truncnorm"
    }
  }
  library {
    cran {
      package = "geosphere"
    }
  }
}

