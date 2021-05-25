# Local development environment

The first steps for getting started participating in the project is to set up a local development environment for python and clone the Git repository.
The current source code is written in Python and R, and we use [Poetry](https://python-poetry.org/) for packaging and dependency management.

## Setup Poetry environment for python (unix)

1. Install poetry and pyenv globally
   - Run `brew install poetry`
   - Run `brew install pyenv`

### Setup Poetry env for python (linux - ubuntu)
   - Run `pip install poetry`
   - Run `sudo apt install python3-venv`

2. Open the repository in VS code. Run the following in terminal
3. Create virtual environment, python3 -m venv .venv.
   - Run `pyenv install 3.9.1` in terminal
   - Run `pyenv local 3.9.1` in terminal
   - Run `poetry install`. All dependencies given py the pyproject.toml file will be installed in your environment

## Cloning git repository

You get a local version of project documentation and source code by gloning the repository:
`git clone https://Kantega-AML@dev.azure.com/Kantega-AML/AI/_git/AI`

This allows you to work on the content locally, and lated submit the changes to the shared repository.

## Pushing local changes

All repository changes should be done within a separate branch. Your branch can be merging into the **development branch** by going through a review and approved pull request.
Instructions for how to create a branch and set correct naming is found [here](/docs/branching.md).

Before the merging you should make sure that the code is formatted properly (using **black** and **isort**). See instructions and command examples below:

### Black

[Black](https://pypi.org/project/black/) will format the python code and fix indenting, wrapping and other formatting issues.
`poetry run black <file or folder>`

### Isort

Isort will sort optimize import statements in the python code.
`poetry run isort <file or folder>`

### Markdownlint

Download the extension `markdownlint` by David Anson. This will prompt warnings when you edit markdown files and lint checks fail

In VS Code Terminal run `code --install-extension DavidAnson.vscode-markdownlint`.

If you get `command not found: code` response, you need to add VS Code to your bash profile. Run the following, and then open a new terminal before installing again.

```bash
cat << EOF >> ~/.bash_profile
# Add Visual Studio Code (code)
export PATH="\$PATH:/Applications/Visual Studio Code.app/Contents/Resources/app/bin"
EOF
```
