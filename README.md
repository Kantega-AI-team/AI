# Repository for AML AI

This repository contains all code for the AI AML project.

It consists of the following components

- [Notebooks runnable in Databricks](notebooks)
- [Examples not runnable as notebooks](examples)
- [Pipelines for PR-checks and notebook deployment](pipelines)
- [Documentation](docs)

The following components have been manually created in Azure to run the code in Databricks

- Storage account: [kantegaaiamldls](https://portal.azure.com/#@Kantega.onmicrosoft.com/resource/subscriptions/2920fa54-dd01-4730-b260-e96a1888bcca/resourceGroups/kantega-aml/providers/Microsoft.Storage/storageAccounts/kantageaamlaidls/overview)
- Key-vault: [kantega-aml-dbw-kv-dev](https://portal.azure.com/#@Kantega.onmicrosoft.com/resource/subscriptions/2920fa54-dd01-4730-b260-e96a1888bcca/resourceGroups/kantega-aml/providers/Microsoft.KeyVault/vaults/kantega-aml-dbw-kv-dev/overview)
- Databricks workspace: [kantega-aml-dbw-dev](https://adb-4911850018174512.12.azuredatabricks.net)
- Service principal: [kantega-aml-dbw-sp](https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/Overview/appId/289b8913-350b-4ce1-b7e1-d38b533e9b9e/isMSAApp/)

# Applied AI Notebooks
You find an overview and introduction to all the [notebook examples here](/docs/notebook_overview.md)