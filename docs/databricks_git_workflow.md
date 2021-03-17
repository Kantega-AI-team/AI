# Databricks git workflow

All the notebook code in the development branch of this repository is automatically published in the Databricks workspace at each update to the branch (on merged pull requests). But you may also want to run code in Databricks during development. This documentation describes the steps for developing notebooks, both in Databricks and locally.

This guide assumes you have already linked the repository to your Databricks user. See the [setup guide](initial_setup_dbw_repos.md).

## Creating a new branch

The first step is always to create a new branch. This can be achieved directly in DevOps, in Databricks or by creating and publishing the branch locally

### Creating a branch locally

You can create a new branch locally in one of two ways

1. Using GitHub Desktop. Create new branch with [correct naming](branching.md), and then press publish.
2. Using command line

    ```bash
    git checkout -b <branch name>
    git push -u origin <branch name>
    ```

### Creating a branch in Azure DevOps

Go to Repos -> Branches -> Create New branch

### Creating a branch in Databricks

Go to Repos -> AI. Press anywhere the selected branch is displayed. The prompt allows you to create a new branch:

![dbwprompt](/docs/images/dbw_repos_create_branch.png)

## Creating a new notebook

There are two ways to initialize a new notebook - either locally or in Databricks.

### Create notebook locally

To create a notebook locally, simply add a `.py` or `.r` file at the desired place in the repository directory, somewhere under `notebooks`. All Databricks notebooks file should start with the Databricks identifier

```# Databricks notebook source```

Ensure you are working on your newly created branch, and add comprehensible commit messages, especially on large diffs.

### Create new notebook on Databricks

To create a new notebook in Databricks, checkout your newly created branch under repos, and work from there.

Keep in mind that the branch is not automatically pushed to remote. To push your code, click the repository button, which will prompt all stagedchanes. Enter good commit message, before pressing 'commit/push'.

## Switching between environments

If you switch between local development and development in Databricks, do a fetch/pull before edits.

## Useful links

[Databricks repos documentation](https://docs.Databricks.com/repos.html)
