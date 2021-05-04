# Repository for Kantega AI Team

This repository contains all code for the AI AML project.

It consists of the following components

- [Notebooks runnable in Databricks](notebooks)
- [Examples not runnable as notebooks](examples)
- [Pipelines for PR-checks and notebook deployment](pipelines)
- [Documentation](docs)

## Applied AI Notebooks

You find an overview and introduction to all the [notebook examples here](/docs/notebook_overview.md)

## Azure infrastructure

All infrastructure is defined  as code. We only deploy in dev. Main is solely used for publishing work, and the code does not run on separate instances for separate environments. See the [infrastructure files](terraform) for details.

## Azure Resource Naming Convensions

Resource names should be a concatication of the following elements:
{ Resources type abbreviation }-{ use case }-{ environment }-{ azure region }-{ instance number }

where the resource type abbreviation should follow: [](https://docs.microsoft.com/en-us/azure/cloud-adoption-framework/ready/azure-best-practices/resource-abbreviations)

Example
"dls-ai-dev-westeu-001"
