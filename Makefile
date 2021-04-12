.ONESHELL:
.PHONY: test
.DEFAULT_GOAL: all

all: install lint test cover

debug:
	pip install . --force --no-deps

install:
	poetry install --remove-untracked

isort:
	poetry run isort src

pylint:
	poetry run pylint src || poetry run pylint-exit $$?

mypy:
	poetry run mypy src

lint: isort pylint mypy

test:
	PYTHONPATH="$$PYTHONPATH:src" poetry run pytest --cov-report=term-missing --cov=aiosignalrcore --cov-report=xml -v .

cover:
	poetry run diff-cover coverage.xml

build:
	poetry build

release-patch:
	bumpversion patch
	git push --tags
	git push

release-minor:
	bumpversion minor
	git push --tags
	git push

release-major:
	bumpversion major
	git push --tags
	git push
