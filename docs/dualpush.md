# Using the dualpush script

The [dualpush script](../utils/dualpush.py) is a useful tool for pushing local code to both Databricks and the git repo. To run the code, follow the guide below

## Prerequisites

To run the script you need the following

- An environment with `databricks_cli` and `gitpython` preinstalled. The [poetry](https://python-poetry.org/) setup included in the repository contains all dependencies.
- The databricks workspace url
- A personal access token from Databricks
- git must be configured in your environment

### Setup poetry environment for python (unix)

1. Install poetry and pyenv globally
   1. Run `brew install poetry`
   2. Run `brew install pyenv`
2. Open the repository in VS code. Run the following in terminal
   1. Create virtual environment, `python3 -m venv .venv`.
   2. run `pyenv install 3.9.1` in terminal to 
   3. run `pyenv local 3.9.1` in terminal
   4. Run `poetry install` . All dependencies given py the `pyproject.toml` file will be installed in your environment

## Run the script

Use poetry to run the python script, which adds some failsafe on python version etc. Insert values and run the following in your terminal

`poetry run python "{local path to dualpush.py}" --host "{databricks workspace url}" --token "{your personal access token}" --notebook-path "{local path to the notebook you are pushing}" --commit-message "{your git commit message}"`
