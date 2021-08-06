# How to contribute to Intake Pattern Catalog

We welcome contributions to this repository.

Here are some useful resources for you:

* [Intake driver
  documentation](https://intake.readthedocs.io/en/latest/making-plugins.html)
* [Issue tracker for this
  repository](https://intake.readthedocs.io/en/latest/making-plugins.html)

## Code of Conduct

This project is governed by a [code of conduct](CODE_OF_CONDUCT.md). By participating in the development of
this project, you are expected to uphold this code.

## Cloning and testing `intake-pattern-catalog`

Clone this repository locally with

```bash
git clone https://.../intake-pattern-catalog.git
```

Assuming you have Python 3 and [make](https://www.gnu.org/software/make/) available on
your machine, you can test with

```bash
make test
```

This roughly does:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --editable .
pip install -r dev-requirements.txt
pytest
```

## Static checks and pre-commit hooks

Our codebase relies on [black](https://black.readthedocs.io/) and
[isort](https://pycqa.github.io/isort/) for automatic code formatting.

The code must also pass checks in the [flake8](https://flake8.pycqa.org/) and
[mypy](https://mypy.readthedocs.io/) libraries.

Code can be formatted with `make format`.

Code can be checked against isort, black, mypy, and flake8 with `make lint`.

You can use the [pre-commit](https://pre-commit.com/) tool to automatically lint your
changes prior to commiting by [installing pre-commit](https://pre-commit.com/#install)
and running `pre-commit install`.

## Submitting Changes

Please ensure your Git commit messages are descriptive and written in present tense. The
first line of each commit message should be 72 characters or fewer.

[Open a pull request](). The pull request should include a descriptive title,
and the body should carefully explain the problem being solved and provide an overview
of the solution.

After submitting the pull request, watch for the completion of the test run
and static checks. Fix any issues uncovered until all checks have passed.