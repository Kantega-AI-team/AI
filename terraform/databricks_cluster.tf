resource "databricks_cluster" "dbwc" {
  cluster_name            = "${var.cluster_prefix}-${var.environment}-${var.standard_suffix}"
  spark_version           = "8.3.x-cpu-ml-scala2.12"
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

