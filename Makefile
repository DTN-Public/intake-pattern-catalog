lint: lint-flake8 lint-mypy lint-isort lint-black

format: format-black format-isort

test: venv
	$(VENV)/pytest

format-black: venv
	$(VENV)/black .

format-isort: venv
	$(VENV)/isort .

lint-flake8: venv
	$(VENV)/flake8 .

lint-black: venv
	$(VENV)/black --check .

lint-mypy: venv
	$(VENV)/mypy

lint-isort: venv
	$(VENV)/isort --check .

lint-yaml: venv
	yamllint -c .yamllint.yaml catalog/*.yaml .yamllint.yaml

dist: clean-dist venv
	$(VENV)/python setup.py sdist bdist_wheel
	ls -l dist

clean-dist:
	rm -fr dist

install-hooks: .git/hooks/pre-commit .pre-commit-config.yaml

.git/hooks/pre-commit:
	pre-commit install

include Makefile.venv.mk

.PHONY: lint format format-* lint-* test clean-dist aws-login install-hooks venv
