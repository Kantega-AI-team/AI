resource "databricks_notebook" "notebook" {
  for_each = local.notebooks
  source   = "${dirname(path.cwd)}/notebooks/${each.value}"
  path     = "/Shared/Demo/${each.value}"
}

locals {
  # Filter out Python and R files in the notebooks subfolders
  notebooks = toset([for value in fileset("../notebooks", "**") : value if length(regexall(".py|.r", value)) > 0])
}

output "notebooks_published" {
  value = local.notebooks
}