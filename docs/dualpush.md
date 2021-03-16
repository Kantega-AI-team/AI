# Using the dualpush script

The [dualpush script](../utils/dualpush.py) is a useful tool for pushing local code to both Databricks and the git repo. To run the code, follow the guide below

## Prerequisites

To run the script you need the following

- An environment with `databricks_cli` and `gitpython` preinstalled. The [poetry](https://python-poetry.org/) setup included in the repository contains all dependencies.
- The databricks workspace url
- A personal access token from Databricks
- git must be configured in your environment

## Run the script

Use poetry to run the python script, which adds some failsafe on python version etc. Insert values and run the following in your terminal

`poetry run python "{local path to dualpush.py}" --host "{databricks workspace url}" --token "{your personal access token}" --notebook-path "{local path to the notebook you are pushing}" --commit-message "{your git commit message}"`

Note that:
- The databricks workspace url must target the root of the workspace, not specific notebook urls.
- Your personal access token must be created in Databricks, under the user setting ![user settings](images/access_token.png =600x500).
- The notebook-path must be a absolute path

Example command

poetry run python utils/dualpush.py --host "https://adb-4911850018174512.12.azuredatabricks.net/?o=4911850018174512" --token "dapia64f0527cc203f3b40c1a7dasdaseradas-2" --notebook-path "/Users/johndoe/Development/AI/notebooks/my_notebook.py" --commit-message "My message"
```