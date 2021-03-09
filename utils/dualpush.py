"""Script for pushing local code to both git and Databricks"""

import argparse
from os import environ
from sys import argv

import databricks_cli
from databricks_cli.sdk import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
from git import Repo

# Get command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--host", type=str, help="Databricks workspace url", required=True)
parser.add_argument(
    "--token",
    type=str,
    help="Personal access token for Databricks workspace",
    required=True,
)
parser.add_argument(
    "--notebook-path", type=str, help="Local path to your notebook", required=True
)
parser.add_argument(
    "--commit-message",
    type=str,
    help="Commit message for git",
    default="Making changes to notebook",
)
args = parser.parse_args()

# Assign args.notebooks_path and assert contains 'notebooks'
path = args.notebook_path
assert "notebooks" in path, "Local path must contain 'notebooks'"

# Set databricks client
client = ApiClient(host=args.host, token=args.token)

# Get local repo path using notebook path
repo_path = path.split("notebooks")[0]

# Set commit message
commit_message = args.commit_message

# Get language by extension
language_ext = path.split(".")[-1]
if language_ext == "py":
    language = "PYTHON"
elif language_ext == "r":
    language = "R"
else:
    raise TypeError(f"Extension {language_ext} not supported")

# Push notebooks to repo
def git_push() -> None:
    repo = Repo(repo_path)
    repo.git.add(path)
    repo.index.commit(commit_message)
    repo.remote().push()


# Push notebook to Databricks
def databricks_push() -> None:
    workspace = WorkspaceApi(client)
    workspace.import_workspace(
        source_path=args.notebook_path,
        target_path=path.split("notebooks")[-1].split(".")[0],
        fmt="SOURCE",
        language=language,
        is_overwrite=True,
    )


if __name__ == "__main__":
    try:
        git_push()
    except Exception as e:
        print("Failed to push to git")
        print(str(e))
    try:
        databricks_push()
    except Exception as e:
        print("Failed to push to Databricks")
        print(str(e))